# agent-connector

**Version: 0.7.2**

Universal Agent Adapter Runtime — transport-neutral middleware that exposes
local (stdio) and remote (HTTP/SSE/MCP) agents through a uniform task lifecycle
with durable storage, idempotency, retention and A2A/ACP protocol mappers.

## Layout

```text
agent-connector/
├── Cargo.toml                      # workspace root (resolver = "2"), version = "0.7.2"
├── config/adapter.example.yaml     # example adapterd configuration
├── crates/
│   ├── adapter-model/              # DTO, identifiers, schema (no runtime)
│   ├── adapter-store-contract/     # TaskStore trait + retention
│   ├── adapter-core/               # task lifecycle, registry, policy
│   ├── protocol-a2a-mapper/        # A2A <-> Core semantic mapper
│   ├── protocol-a2a-server/        # A2A HTTP router, health/readiness
│   ├── protocol-acp-mapper/        # ACP <-> Core semantic mapper
│   ├── protocol-acp-runtime/       # ACP stdio runtime (lib; launch deferred)
│   ├── driver-a2a-client/          # A2A client (SDK/Spec wire, dialect probe)
│   ├── driver-acp-client/          # ACP stdio client
│   ├── driver-stdio/               # UAIC/1 NDJSON subprocess driver
│   ├── driver-http-sse/            # UAIC/1 HTTP+SSE driver
│   ├── driver-mcp/                 # MCP client driver (rmcp 0.8.5)
│   ├── memory-task-store/          # in-memory TaskStore (tests/demo)
│   ├── sqlite-task-store-adapter/  # durable single-node TaskStore
│   ├── postgres-task-store-adapter/# multi-instance TaskStore
│   ├── adapterd/                   # composition root / daemon binary
│   └── adapterctl/                 # installer / service manager CLI
├── docs/                           # entry docs + design/ (source specs)
├── tests/                          # contract / integration / fixtures
├── scripts/                        # check.sh, run-local.sh
└── deploy/                         # docker-compose.postgres.yaml, systemd unit
```

## Architecture

![Architecture diagram](docs/architecture.svg)

6 layers: External Clients → Protocol Servers → Core Runtime → Storage → Drivers → Binaries.

## Quick start (SQLite, local)

```bash
cp config/adapter.example.yaml adapter.yaml
./scripts/check.sh                  # fmt + clippy + test
cargo run -p adapterd -- adapter.yaml
```

Adapterd starts, creates `./data/adapter.db`, launches agents from config
(stdio / HTTP+SSE / MCP transport) and runs background retention-cleanup.
Logs are written via `tracing`.

## Drivers

| Driver | Transport | Status |
|---|---|---|
| `driver-stdio` | UAIC/1 NDJSON subprocess | ready |
| `driver-http-sse` | UAIC/1 HTTP+SSE | ready |
| `driver-a2a-client` | A2A SDK/Spec wire, dialect probe | ready |
| `driver-acp-client` | ACP stdio client | ready |
| `driver-mcp` | MCP (rmcp 0.8.5), stdio + HTTP | ready; discovery/invoke/progress/cancel via `CancellationToken`, hot-update skills |

`driver-mcp` connects to any MCP server via stdio, discovers tools via
`list_tools`, invokes them via `send_request_with_option` + `RequestHandle`,
subscribes to progress events via built-in `ProgressDispatcher`, and supports
cancel — `RequestHandle` stays inside the spawned task, only `CancellationToken`
is passed externally to avoid moved-value conflict between `await_response(self)`
and `cancel(self, ..)`.

## Installer: adapterctl

`crates/adapterctl` — installer / service manager (Linux systemd, macOS
launchd, Windows sc.exe):

```bash
sudo adapterctl install --storage sqlite --start     # install as service
sudo adapterctl restart                               # restart
sudo adapterctl uninstall --purge-data                # remove with data
```

Managed Docker Postgres installation requires `--confirm-docker`; secrets
(DSN, bearer tokens) are written only to `.env` next to the config — never in
`adapter.yaml`. Details: `docs/user-guide.md`.

## Design documents

Source specs and architecture are in `docs/design/`:

- `adapter_core.rs` — runtime architecture
- `adapter_core_v2.rs` — module specs
- `adapter_core_v2_fix.rs` — production NFR
- `adapter_store_contract.rs` — store contract
- `adapterd_config.rs` — configuration format

Overview: `docs/architecture.md`. Operations: `docs/operations.md`.
Protocol compatibility: `docs/protocol-compatibility.md`.
User guide: `docs/user-guide.md`. Contributing: `docs/contributing.md`.

## A2A Protocol Strategy 2026

Protocol-dialect strategy for the gateway and the adapter (A2A SDK v1.0 = base,
Spec pre-1.0 = fallback, ACP = deep fallback, ANP — out of scope). Pick a
language — each opens a short summary linking to the full strategy in that
language:

- **EN:** [A2A-protocol-strategy-2026-en.summary.md](docs/A2A-protocol-strategy-2026-en.summary.md)
- **UA:** [A2A-protocol-strategy-2026-uk.summary.md](docs/A2A-protocol-strategy-2026-uk.summary.md)
- **RU:** [A2A-protocol-strategy-2026-ru.summary.md](docs/A2A-protocol-strategy-2026-ru.summary.md)

## Status

Version **0.7.2**. Workspace, crates migrated from flat prototype
(`adapter_core_v2_fix.rs` → `adapter-core`, DTO → `adapter-model`),
`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace` all pass.

Implemented: A2A server (`protocol-a2a-server`), ACP runtime
(`protocol-acp-runtime`, lib; launch deferred), A2A/ACP client drivers
(`driver-a2a-client` with dialect probe SDK/Spec, `driver-acp-client`), MCP driver
(`driver-mcp`, rmcp 0.8.5, stdio + HTTP transports, progress/cancel via
`CancellationToken`, input-schema validation, protocol version check,
hot-update skills on `tools/list_changed`), installer (`adapterctl`) with
launchd/sc.exe/systemd layers and graceful shutdown.

Known MCP limitations: hot-update skills on `tools/list_changed`
implemented (ADR-0001 R1, commit `625545b`), multi-turn `input_required`
not supported by driver (`provide_input` → error), prompts/resources
not mapped — see `docs/design/adr-0001-mcp-dynamic-capabilities.md`.
