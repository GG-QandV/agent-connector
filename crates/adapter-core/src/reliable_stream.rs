//! `ReliableTaskStream` — a single reconnect/lag/gap-safe event stream on
//! top of `AdapterCore::subscribe`. Wraps the raw `TaskSubscription`
//! (durable history + live `broadcast::Receiver<CoreEvent>`) and guarantees:
//!
//! - no event with `seq <= after_seq` (already acknowledged) is redelivered;
//! - a `broadcast::error::RecvError::Lagged` never surfaces to callers —
//!   it triggers durable catch-up via `EventHistorySource::history`;
//! - a sequence gap (received `seq` > `last_seq + 1`) also triggers durable
//!   catch-up instead of skipping events;
//! - duplicate `seq` values (already delivered from history or a previous
//!   catch-up) are silently dropped;
//! - after a terminal event (`Completed`/`Failed`/`Cancelled`) is delivered,
//!   `next()` always returns `Ok(None)` — no further reconnect is attempted.
//!
//! This module is protocol-neutral (no A2A/ACP/ANP dependency). Protocol
//! mappers should replace direct `TaskSubscription`/`broadcast::Receiver`
//! consumption with this type; see `docs/reliable-task-stream-wiring.md` for
//! the two-line wiring needed in `lib.rs`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use adapter_model::{CoreEvent, CoreEventKind, EventSeq, TaskId};
use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::{AdapterCore, CoreError, TaskSubscription};

/// Abstraction over "read durable event history strictly after a given
/// seq". Implemented for `AdapterCore` below; a fake implementation can be
/// used in unit tests without a real `TaskStore`/driver/worker stack.
#[async_trait]
pub trait EventHistorySource: Send + Sync {
    async fn history(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
    ) -> Result<Vec<CoreEvent>, CoreError>;
}

#[async_trait]
impl EventHistorySource for AdapterCore {
    async fn history(
        &self,
        task_id: TaskId,
        after_seq: EventSeq,
    ) -> Result<Vec<CoreEvent>, CoreError> {
        AdapterCore::history(self, task_id, after_seq).await
    }
}

fn is_terminal(kind: &CoreEventKind) -> bool {
    matches!(
        kind,
        CoreEventKind::Completed { .. } | CoreEventKind::Failed { .. } | CoreEventKind::Cancelled
    )
}

/// Reliable, reconnect/lag/gap-safe wrapper around a single task's event
/// stream. One instance is single-use: once terminal, create a new one for a
/// new logical subscription (e.g. on a fresh client reconnect).
pub struct ReliableTaskStream {
    source: Arc<dyn EventHistorySource>,
    task_id: TaskId,
    last_seq: EventSeq,
    pending: VecDeque<CoreEvent>,
    receiver: broadcast::Receiver<CoreEvent>,
    terminal: bool,
}

impl ReliableTaskStream {
    /// `after_seq` is the last event the caller has already durably
    /// processed. Events already present in `subscription.history` with
    /// `seq <= after_seq` are dropped; the rest are queued for delivery
    /// before falling through to the live receiver.
    pub fn new(
        source: Arc<dyn EventHistorySource>,
        task_id: TaskId,
        after_seq: EventSeq,
        subscription: TaskSubscription,
    ) -> Self {
        let pending = subscription
            .history
            .into_iter()
            .filter(|event| event.seq > after_seq)
            .collect();
        Self {
            source,
            task_id,
            last_seq: after_seq,
            pending,
            receiver: subscription.receiver,
            terminal: false,
        }
    }

    /// Convenience constructor: calls `core.subscribe(task_id, after_seq)`
    /// and wraps the resulting `TaskSubscription`. Does not require any
    /// change to `AdapterCore`'s existing `impl` block.
    pub async fn subscribe(
        core: &AdapterCore,
        task_id: TaskId,
        after_seq: EventSeq,
    ) -> Result<Self, CoreError> {
        let subscription = core.subscribe(task_id, after_seq).await?;
        Ok(Self::new(
            Arc::new(core.clone()),
            task_id,
            after_seq,
            subscription,
        ))
    }

    /// Last `seq` successfully delivered to the caller (or the initial
    /// `after_seq` if nothing has been delivered yet). Callers should persist
    /// this value as their reconnect checkpoint.
    pub fn last_seq(&self) -> EventSeq {
        self.last_seq
    }

    /// `true` once a terminal event has been delivered; `next()` will always
    /// return `Ok(None)` afterwards.
    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    /// Returns the next event in strict `seq` order, or `Ok(None)` once the
    /// task has reached a terminal state and no more events will ever
    /// arrive.
    ///
    /// This method never surfaces a `broadcast::error::RecvError` — lag and
    /// sequence gaps are resolved internally via durable catch-up.
    pub async fn next(&mut self) -> Result<Option<CoreEvent>, CoreError> {
        loop {
            if self.terminal {
                return Ok(None);
            }
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(self.deliver(event)));
            }
            match self.receiver.recv().await {
                Ok(event) => {
                    if event.seq <= self.last_seq {
                        // Duplicate: already delivered from history or a
                        // previous catch-up.
                        continue;
                    }
                    if event.seq == self.last_seq + 1 {
                        return Ok(Some(self.deliver(event)));
                    }
                    // Gap: do not skip ahead on live-channel arrival order
                    // alone. Re-read durable history from the last
                    // acknowledged seq, which is authoritative.
                    self.catch_up(Some(event)).await?;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Never surface Lagged: the live channel dropped events,
                    // but the durable store did not.
                    self.catch_up(None).await?;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // Sender side gone (task worker finished/dropped). Do one
                    // last durable read in case the terminal event was
                    // written to the store but missed on the live channel.
                    self.catch_up(None).await?;
                    if self.pending.is_empty() {
                        self.terminal = true;
                        return Ok(None);
                    }
                }
            }
        }
    }

    fn deliver(&mut self, event: CoreEvent) -> CoreEvent {
        self.last_seq = event.seq;
        if is_terminal(&event.kind) {
            self.terminal = true;
        }
        event
    }

    /// Reads durable history strictly after `self.last_seq`, merges in an
    /// optional trailing live event, deduplicates by `seq` and refills
    /// `pending` in ascending order.
    async fn catch_up(&mut self, trailing: Option<CoreEvent>) -> Result<(), CoreError> {
        let events = self.source.history(self.task_id, self.last_seq).await?;
        let mut merged: BTreeMap<EventSeq, CoreEvent> = BTreeMap::new();
        for event in events {
            if event.seq > self.last_seq {
                merged.insert(event.seq, event);
            }
        }
        if let Some(event) = trailing {
            if event.seq > self.last_seq {
                merged.entry(event.seq).or_insert(event);
            }
        }
        self.pending = merged.into_values().collect();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adapter_model::PublicError;
    use chrono::Utc;
    use std::sync::Mutex;

    fn progress_event(task_id: TaskId, seq: EventSeq) -> CoreEvent {
        CoreEvent {
            task_id,
            seq,
            at: Utc::now(),
            kind: CoreEventKind::Progress {
                message: format!("step {seq}"),
                percent: None,
            },
        }
    }

    fn completed_event(task_id: TaskId, seq: EventSeq) -> CoreEvent {
        CoreEvent {
            task_id,
            seq,
            at: Utc::now(),
            kind: CoreEventKind::Completed { output: vec![] },
        }
    }

    fn failed_event(task_id: TaskId, seq: EventSeq) -> CoreEvent {
        CoreEvent {
            task_id,
            seq,
            at: Utc::now(),
            kind: CoreEventKind::Failed {
                error: PublicError {
                    code: "test".into(),
                    message: "boom".into(),
                    retryable: false,
                },
            },
        }
    }

    /// Fake durable store: an in-memory, append-only `Vec<CoreEvent>` shared
    /// behind a `Mutex`, standing in for `TaskStore::events_after` without
    /// requiring a real `AdapterCore`/driver/worker stack.
    struct FakeHistory {
        events: Mutex<Vec<CoreEvent>>,
    }

    impl FakeHistory {
        fn new(events: Vec<CoreEvent>) -> Arc<Self> {
            Arc::new(Self {
                events: Mutex::new(events),
            })
        }

        fn push(&self, event: CoreEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[async_trait]
    impl EventHistorySource for FakeHistory {
        async fn history(
            &self,
            task_id: TaskId,
            after_seq: EventSeq,
        ) -> Result<Vec<CoreEvent>, CoreError> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.task_id == task_id && event.seq > after_seq)
                .cloned()
                .collect())
        }
    }

    fn subscription_with(
        history: Vec<CoreEvent>,
        receiver: broadcast::Receiver<CoreEvent>,
    ) -> TaskSubscription {
        TaskSubscription {
            history,
            receiver,
            history_end_seq: None,
        }
    }

    #[tokio::test]
    async fn delivers_history_then_live_in_order_without_duplicates() {
        let task_id = TaskId::new_v4();
        let (tx, rx) = broadcast::channel(16);
        let history = vec![progress_event(task_id, 1), progress_event(task_id, 2)];
        let source = FakeHistory::new(history.clone());
        let mut stream =
            ReliableTaskStream::new(source, task_id, 0, subscription_with(history, rx));

        assert_eq!(stream.next().await.unwrap().unwrap().seq, 1);
        assert_eq!(stream.next().await.unwrap().unwrap().seq, 2);

        // Live event that duplicates history (already delivered) must be
        // silently dropped, not redelivered.
        tx.send(progress_event(task_id, 2)).unwrap();
        tx.send(progress_event(task_id, 3)).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().seq, 3);
        assert_eq!(stream.last_seq(), 3);
    }

    #[tokio::test]
    async fn resume_after_seq_skips_already_acknowledged_history() {
        let task_id = TaskId::new_v4();
        let (_tx, rx) = broadcast::channel(16);
        let history = vec![
            progress_event(task_id, 1),
            progress_event(task_id, 2),
            progress_event(task_id, 3),
        ];
        let source = FakeHistory::new(history.clone());
        let mut stream =
            ReliableTaskStream::new(source, task_id, 2, subscription_with(history, rx));

        // Only seq=3 should be delivered; seq<=2 is already acknowledged.
        assert_eq!(stream.next().await.unwrap().unwrap().seq, 3);
        assert_eq!(stream.last_seq(), 3);
    }

    #[tokio::test]
    async fn lagged_receiver_recovers_all_events_from_durable_history() {
        let task_id = TaskId::new_v4();
        let (tx, rx) = broadcast::channel(4);
        let source = FakeHistory::new(vec![]);
        let mut stream =
            ReliableTaskStream::new(source.clone(), task_id, 0, subscription_with(vec![], rx));

        // Overflow the small broadcast channel so the receiver lags, while
        // durably recording every event in the fake store (as
        // `append_event_and_transition` would before `tx.send` in
        // production).
        for seq in 1..=10u64 {
            let event = progress_event(task_id, seq);
            source.push(event.clone());
            let _ = tx.send(event);
        }

        let mut received = Vec::new();
        loop {
            match stream.next().await.unwrap() {
                Some(event) => received.push(event.seq),
                None => break,
            }
            if received.len() == 10 {
                break;
            }
        }

        assert_eq!(received, (1..=10).collect::<Vec<_>>());
        assert_eq!(stream.last_seq(), 10);
    }

    #[tokio::test]
    async fn sequence_gap_triggers_durable_catch_up_instead_of_skipping() {
        let task_id = TaskId::new_v4();
        let (tx, rx) = broadcast::channel(16);
        let source = FakeHistory::new(vec![]);
        let mut stream =
            ReliableTaskStream::new(source.clone(), task_id, 0, subscription_with(vec![], rx));

        // Durable store has seq 1..3, but the live channel only delivers 4:
        // a naive implementation would skip straight to 4 and lose 1..3.
        source.push(progress_event(task_id, 1));
        source.push(progress_event(task_id, 2));
        source.push(progress_event(task_id, 3));
        tx.send(progress_event(task_id, 4)).unwrap();

        let mut received = Vec::new();
        for _ in 0..4 {
            received.push(stream.next().await.unwrap().unwrap().seq);
        }

        assert_eq!(received, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn terminal_completed_event_stops_the_stream() {
        let task_id = TaskId::new_v4();
        let (tx, rx) = broadcast::channel(16);
        let source = FakeHistory::new(vec![]);
        let mut stream = ReliableTaskStream::new(source, task_id, 0, subscription_with(vec![], rx));

        tx.send(progress_event(task_id, 1)).unwrap();
        tx.send(completed_event(task_id, 2)).unwrap();

        assert_eq!(stream.next().await.unwrap().unwrap().seq, 1);
        let terminal = stream.next().await.unwrap().unwrap();
        assert!(matches!(terminal.kind, CoreEventKind::Completed { .. }));
        assert!(stream.is_terminal());

        // No reconnect/further recv attempted once terminal.
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn terminal_failed_event_is_recovered_from_history_after_channel_closed() {
        let task_id = TaskId::new_v4();
        let (tx, rx) = broadcast::channel(16);
        let source = FakeHistory::new(vec![]);
        let mut stream =
            ReliableTaskStream::new(source.clone(), task_id, 0, subscription_with(vec![], rx));

        // Terminal event is durably recorded but the sender is dropped
        // (worker finished) before the live channel actually delivered it —
        // e.g. subscriber connected in the narrow race window.
        source.push(failed_event(task_id, 1));
        drop(tx);

        let terminal = stream.next().await.unwrap().unwrap();
        assert!(matches!(terminal.kind, CoreEventKind::Failed { .. }));
        assert!(stream.is_terminal());
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn closed_channel_with_no_pending_history_ends_stream_cleanly() {
        let task_id = TaskId::new_v4();
        let (tx, rx) = broadcast::channel(16);
        let source = FakeHistory::new(vec![]);
        let mut stream = ReliableTaskStream::new(source, task_id, 0, subscription_with(vec![], rx));
        drop(tx);

        assert!(stream.next().await.unwrap().is_none());
        assert!(stream.is_terminal());
    }
}
