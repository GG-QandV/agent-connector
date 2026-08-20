# Wiring `ReliableTaskStream` into `adapter-core`

`crates/adapter-core/src/reliable_stream.rs` is self-contained: it only uses
already-`pub` items from `crates/adapter-core/src/lib.rs`
(`AdapterCore::subscribe`, `AdapterCore::history`, `AdapterCore` being
`Clone`, and the `pub struct TaskSubscription`). It does not require editing
`AdapterCore`'s existing `impl` block.

`lib.rs` was not rewritten directly in this change because the file could
only be retrieved as partial excerpts through available tooling, and
blindly resubmitting the full ~34KB file risked silently dropping code that
was not visible (worker/driver dispatch loop, `AgentRegistry`, full
`invoke`/`cancel`/`provide_input` bodies, existing `#[cfg(test)]` module).
Apply this two-line change by hand (or via a small, reviewed diff) instead
of a full-file replace:

```rust
// near the other `mod` declarations, e.g. after `mod bearer_token;`
mod reliable_stream;
pub use reliable_stream::{EventHistorySource, ReliableTaskStream};
```

No other changes to `lib.rs` are required. Callers migrate incrementally:

```rust
// Before (direct TaskSubscription/broadcast consumption, e.g. in a
// protocol mapper):
let subscription = core.subscribe(task_id, after_seq).await?;
match subscription.receiver.recv().await { /* Lagged handled ad hoc */ }

// After:
let mut stream = ReliableTaskStream::subscribe(&core, task_id, after_seq).await?;
while let Some(event) = stream.next().await? {
    // event.seq is strictly increasing, no duplicates, no gaps;
    // Lagged is resolved internally via durable catch-up.
}
// stream.is_terminal() is true once next() has returned Ok(None).
```

## Suggested follow-up PRs (not part of this change)

1. Migrate `protocol-a2a-mapper::A2aTaskEventStream::next()` to wrap
   `ReliableTaskStream` instead of calling `receiver.recv()` directly.
2. Migrate `protocol-acp-mapper::AcpUpdateStream::next()` the same way.
3. Once both are migrated, the ad hoc `Err(error) => Err(error) // wire
   layer must re-read durable history after lag` comments in both mappers
   can be removed — the recovery is now centralized.
