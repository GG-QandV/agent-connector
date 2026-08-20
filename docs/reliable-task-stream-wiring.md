# Wiring `ReliableTaskStream` into `adapter-core`

`crates/adapter-core/src/reliable_stream.rs` is self-contained: it only uses
already-`pub` items from `crates/adapter-core/src/lib.rs`
(`AdapterCore::subscribe`, `AdapterCore::history`, `AdapterCore` being
`Clone`, and the `pub struct TaskSubscription`). It does not require editing
`AdapterCore`'s existing `impl` block.

## Applied wiring (done)

`crates/adapter-core/src/lib.rs` exposes the module:

```rust
mod reliable_stream;
pub use reliable_stream::{EventHistorySource, ReliableTaskStream};
```

Both protocol mappers were migrated to consume `ReliableTaskStream` instead
of raw `TaskSubscription`/`broadcast::Receiver`:

- `protocol-a2a-mapper::A2aTaskEventStream` — wraps `ReliableTaskStream`;
  `next()` returns `Result<Option<A2aStreamEvent>, A2aMapperError>`; exposes
  `last_seq()` for reconnect checkpoints.
- `protocol-acp-mapper::AcpUpdateStream` — wraps `ReliableTaskStream`;
  `next()` returns `Result<Option<Vec<AcpSessionUpdate>>, AcpMapperError>`;
  exposes `last_seq()`.

Each mapper's `A2aCoreService`/`AcpCoreService` trait gained a `history`
method, and a local `HistorySource<C>` adapter implements
`EventHistorySource` for the generic core wrapper. Lag/gap recovery is now
centralized in `ReliableTaskStream`; the previous ad hoc
`Err(error) => Err(error) // wire layer must re-read durable history after
lag` handling in both mappers is removed.

## Migration pattern for callers

```rust
// Before (direct TaskSubscription/broadcast consumption):
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

Note: `ReliableTaskStream::subscribe` requires a concrete `&AdapterCore`.
Generic mappers that abstract core behind a service trait must supply an
`EventHistorySource` adapter (see the `HistorySource<C>` pattern above) and
bound `C: 'static`.