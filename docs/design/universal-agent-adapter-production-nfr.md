# Дополнение к спекам: Production NFR — нагрузка, streaming, backpressure и надёжность

**Назначение:** вставить как отдельный раздел в `universal-agent-adapter-module-specifications.md`.

Этот раздел обязателен для remote MVP и production. Он фиксирует поведение runtime при высокой нагрузке, медленных клиентах, тяжёлом трафике, длинных stream, недоступных агентах, reconnect storm и storage pressure.

---

## 21. Production NFR: нагрузка и streaming reliability

### 21.1 Нормативные принципы

1. Adapter не должен потреблять неограниченную RAM из-за задач, очередей, SSE-клиентов, event stream или artifacts.
2. Медленный caller не должен замедлять agent, task lifecycle или других callers.
3. Один неисправный agent не должен блокировать другие registered agents.
4. Потеря transport connection не должна автоматически означать потерю task.
5. Retry/fallback не должен повторно выполнить side effect.
6. Durable task/event state важнее live delivery: сначала transaction/journal, потом publish.
7. Progress/telemetry — best effort; terminal events и state transitions — durable и обязательные.
8. Все лимиты должны быть configurable, иметь безопасные defaults и быть видимы через metrics.

---

### 21.2 Лимиты ресурсов

Runtime обязан применять лимиты минимум на четырёх уровнях:

```text
global runtime
  └─ caller / tenant
       └─ registered agent
            └─ task / subscriber / connection
```

Минимальная конфигурация:

```yaml
limits:
  global:
    max_concurrent_tasks: 128
    max_open_streams: 512
    max_queue_depth: 1000

  caller:
    max_concurrent_tasks: 8
    max_open_streams: 16
    requests_per_minute: 120

  agent:
    default_max_concurrent_tasks: 4
    default_max_queue_depth: 32

  task:
    max_input_bytes: 1048576
    max_context_bytes: 4194304
    max_event_bytes: 262144
    max_artifact_metadata_bytes: 65536
    max_events_per_task: 10000

  subscriber:
    max_pending_events: 256
    max_pending_bytes: 1048576
```

Значения являются defaults, а не жёсткими требованиями. Для каждого `RegisteredAgent` допускается override, но нельзя назначить лимит выше глобального без явной privileged policy.

---

### 21.3 Backpressure и очередь задач

#### Требования

- Все command queues должны быть bounded.
- Очередь не должна быть неявной и бесконечной.
- Scheduler обязан иметь global, per-caller и per-agent concurrency control.
- Network/DB I/O не выполняется под global registry lock.
- При перегрузке adapter возвращает явную ошибку, а не ждёт бесконечно.

```rust
pub enum QueuePolicy {
    Reject,
    QueueUntilDeadline,
}
```

Default: `Reject`.

При переполнении:

```json
{
  "code": "resource_exhausted",
  "message": "Agent queue is full",
  "retry_after_ms": 1000
}
```

#### Политика очереди

`QueueUntilDeadline` допустим только если:

- caller задал `deadline_at`;
- policy разрешает queue для выбранного agent;
- queue depth находится ниже per-agent limit;
- task не имеет side effect, который требует немедленного ответа, либо caller это явно подтвердил.

Task в очереди имеет state `Accepted`, но не `Running`. Event journal фиксирует `accepted` с причиной `queued`.

---

### 21.4 Медленные SSE/WS/gRPC subscribers

#### Риск

Агент может производить события быстрее, чем внешний клиент читает SSE, WebSocket или gRPC stream. Если runtime буферизует поток без ограничений, память будет расти до OOM.

#### Требования

- У каждого subscriber есть bounded buffer по числу events и bytes.
- Subscriber получает только события в порядке `seq`.
- Отставший subscriber не замедляет event pump, task или других subscribers.
- При превышении лимита subscriber отключается с причиной `slow_consumer`.
- Terminal task/event не теряется: subscriber обязан reconnect и выполнить durable replay из `TaskStore`.

```text
Agent event
  → durable journal write
  → non-blocking publish to active subscribers
       ├─ fast subscriber: immediate send
       └─ slow subscriber: bounded buffer overflow → disconnect
```

#### Поведение transport layers

- HTTP/SSE: закрыть stream; при возможности отправить final SSE event `adapter.error` с `slow_consumer`.
- WebSocket: close с application-defined close code и `slow_consumer` reason.
- gRPC: terminate stream with resource-exhausted status.

Повторное подключение:

```text
GET /events?after_seq=<last_received_seq>
```

Adapter сначала возвращает durable events batch-ами, затем подключает caller к live fan-out.

---

### 21.5 Event journal pressure и progress coalescing

#### Риск

LLM/coding agent может писать token-level или tool-level progress тысячи раз в секунду. Durable запись каждого события создаёт нагрузку на SQLite/Postgres и делает replay дорогим.

#### Классы событий

| Класс           | Примеры                                                          | Durable                      |
| --------------- | ---------------------------------------------------------------- | ----------------------------:|
| Critical        | accepted, input_required, artifact, completed, failed, cancelled | Всегда                       |
| Progress        | status text, percent, token count, heartbeat                     | Coalesced                    |
| Debug telemetry | trace detail, internal timing                                    | Не в task journal по default |

#### Coalescing policy

```yaml
events:
  progress:
    min_interval_ms: 500
    persist_on_percent_change: true
    max_per_task: 1000
    live_delivery: true
```

Rules:

- Сохранять progress не чаще `min_interval_ms`.
- Сохранять progress при смене процента или significant status change.
- Последний progress snapshot хранится в `tasks.current_progress`.
- После `max_per_task` adapter перестаёт journal'ить non-critical progress и увеличивает metric `progress_events_dropped_total`.
- Critical events никогда не coalesce и не drop.

#### Journal retention

```yaml
retention:
  terminal_task_ttl: 7d
  event_ttl: 7d
  artifact_ttl: 3d
  cleanup_interval: 1h
```

Retention cleanup не может удалять task/event, пока задача non-terminal или существует active subscription/retention hold.

---

### 21.6 Artifacts и тяжёлый трафик

#### Обязательное правило

`CoreEvent` и A2A/ACP event не должны содержать raw binary artifact bytes.

```rust
pub struct ArtifactRef {
    pub artifact_id: String,
    pub name: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub checksum: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}
```

#### Artifact flow

```text
Agent output stream
  → streaming upload / file write
  → ArtifactStore returns ArtifactRef
  → task journal stores only ArtifactRef
  → caller downloads through authenticated endpoint or signed short-lived URL
```

#### Требования

- Streaming upload/download; нельзя собирать большой artifact целиком в RAM.
- Configurable max artifact size и max total artifact bytes per task/caller.
- Проверка `Content-Length`, stream byte counter и abort при лимите.
- Optional checksum для integrity.
- Download требует отдельной authorization check.
- Поддержка HTTP Range желательна для больших artifacts.
- Artifact cleanup по TTL и deletion retry.

Storage implementations:

- local profile: filesystem store с path isolation;
- production: S3-compatible object store через отдельный `ArtifactStore` trait.

---

### 21.7 Таймауты и зависшие агенты

Каждый driver обязан поддерживать отдельные таймауты:

```yaml
timeouts:
  connect: 5s
  request_write: 15s
  first_event: 30s
  idle_stream: 60s
  task_default: 15m
  cancel_grace: 10s
  shutdown_grace: 30s
```

| Timeout         | Смысл                          | Реакция                                                        |
| --------------- | ------------------------------ | -------------------------------------------------------------- |
| `connect`       | Нет соединения с агентом       | driver error, fallback только до invoke acceptance             |
| `request_write` | Команда не передалась          | retry/fallback согласно idempotency policy                     |
| `first_event`   | Нет `accepted`/first event     | status probe; затем safe retry только с тем же idempotency key |
| `idle_stream`   | Stream открыт, но агент молчит | status/resume probe; затем cancel policy                       |
| `task_default`  | Общий deadline задачи          | `CancelRequested` → grace → terminal failure/cancel            |
| `cancel_grace`  | Агент не подтвердил cancel     | force policy для конкретного driver                            |

#### Force cancellation

- `stdio`: допускается SIGTERM, затем SIGKILL только после grace period и только по per-agent policy.
- HTTP/SSE: нельзя «убить» удалённый процесс; вызвать cancel/status, затем зафиксировать `Failed(DeadlineExceeded)` или `CancelledByPolicy`.
- После terminal transition поздние driver events не меняют state, но логируются как protocol violation.

---

### 21.8 Reconnect storm и replay protection

#### Риск

После сбоя proxy, сети или deploy много subscribers могут одновременно reconnect и запрашивать полный event history, перегружая DB и network.

#### Требования

- Exponential backoff with full jitter на client/driver reconnect.
- Per-caller/per-IP reconnect rate limit.
- Max replay events и max replay bytes на один request.
- Cursor-based pagination для длинной истории.
- Response `429`/`503` должен включать `Retry-After`.
- Не разрешать unlimited `after_seq=0`, если история превышает configured retention/replay limit.
- Если requested events уже очищены по retention: вернуть `history_expired` + current task snapshot, а не читать несуществующую историю.

```yaml
replay:
  max_events_per_request: 500
  max_bytes_per_request: 4194304
  reconnect_attempts_per_minute: 20
  initial_backoff_ms: 250
  max_backoff_ms: 30000
```

---

### 21.9 Database и storage pressure

#### Требования

- State transition + critical event записываются синхронно и атомарно.
- Progress events могут batch/coalesce, но не terminal events.
- DB transactions короткие; network calls запрещены внутри transaction.
- Connection pool bounded.
- Retry DB transaction только для transient serialization/deadlock failures, с bounded backoff.
- Не выполнять unbounded `SELECT * FROM task_events`.
- Все replay queries используют `(task_id, seq)` index и limit.

#### SQLite profile

- Включить WAL mode.
- Установить busy timeout.
- Один write-heavy node; multi-instance SQLite запрещён.
- При write pressure применять progress coalescing и reject queue policy раньше, чем допустить долгую lock contention.

#### Postgres profile

- Optimistic revision или row-level lock для transition.
- Lease ownership для active task worker.
- Partial indexes для active tasks и expired leases.
- Connection pool size согласуется с max concurrent tasks.
- Event table partitioning/archive — future requirement при большом retention/RPS.

### 21.10 Lease и recovery после crash

Для multi-instance режима:

```text
worker acquires task lease
  → invokes/resumes driver
  → renews lease while task active
  → terminal state releases lease
```

При crash worker lease истекает. Новый worker:

1. читает task state;
2. проверяет capability `resume` у driver;
3. вызывает `resume(task_id, last_seq)` или `get_status`;
4. продолжает event pump либо переводит task в controlled terminal failure;
5. не запускает новый invoke без idempotency/recovery guarantee.

Lease expiration не должен автоматически означать повторное исполнение non-idempotent task.

---

### 21.11 Head-of-line blocking и fairness

#### Риск

Долгие задачи одного caller/agent занимают worker slots и задерживают короткие задачи других callers/agents.

#### Требования

- Отдельные queues и semaphores per agent.
- Per-caller quotas.
- Global quota — только общий верхний предел, не единственная очередь.
- Long task не держит global lock.
- Priority scheduling optional; default FIFO внутри per-agent queue.
- Если priority включён, требуется anti-starvation: ageing или max wait guarantee.
- Healthcheck не проходит через перегруженную execution queue.

---

### 21.12 Graceful shutdown

При shutdown adapter обязан:

1. перестать принимать новые non-health requests;
2. вернуть readiness=false;
3. сохранить state активных задач;
4. перестать назначать новые leases;
5. попытаться graceful cancel/flush в пределах `shutdown_grace`;
6. закрыть SSE/WS/gRPC subscribers с retryable shutdown signal;
7. сохранить event cursor и закрыть storage;
8. дать новому instance resume active tasks через lease recovery.

Local `stdio` child process не должен убиваться немедленно без per-agent shutdown policy.

---

### 21.13 Обязательные metrics и alerts

#### Metrics

```text
adapter_active_tasks{agent,state}
adapter_queue_depth{agent}
adapter_queue_rejected_total{agent,caller}
adapter_slow_consumer_disconnect_total{transport}
adapter_event_journal_write_latency_seconds
adapter_event_journal_write_failures_total
adapter_progress_events_coalesced_total
adapter_progress_events_dropped_total
adapter_sse_reconnect_total{driver}
adapter_replay_requests_total
adapter_replay_history_expired_total
adapter_driver_idle_timeout_total{driver}
adapter_task_deadline_exceeded_total{agent}
adapter_artifact_bytes_total{agent}
adapter_artifact_rejected_total{reason}
adapter_lease_recovery_total
adapter_db_pool_in_use
```

#### Минимальные alert conditions

- queue rejection растёт стабильно;
- journal write latency/failed writes выше baseline;
- slow consumer disconnect spike;
- idle stream timeout spike;
- agent unhealthy/circuit open;
- DB pool exhausted;
- replay history expired unexpectedly;
- memory usage растёт при стабильном RPS.

---

### 21.14 Required load and failure tests

| Сценарий                        | Критерий                                                       |
| ------------------------------- | -------------------------------------------------------------- |
| Много независимых tasks         | Memory bounded; разные агенты не блокируют друг друга          |
| Один медленный SSE client       | Он отключается; task и fast subscribers продолжают работу      |
| 1000+ progress events           | Journal coalescing работает; terminal event не теряется        |
| Большой artifact                | Adapter не держит файл целиком в RAM                           |
| Потеря SSE после `Accepted`     | Task не стартует повторно; reconnect/resume продолжает stream  |
| Reconnect storm                 | DB/network ограничены rate limit и replay batch limit          |
| Agent hangs                     | Срабатывает idle/task timeout и controlled terminal transition |
| SQLite restart                  | Terminal events и task snapshot сохраняются                    |
| Worker crash в Postgres profile | Lease recovery не создаёт duplicate invoke                     |
| Один failing agent              | Другие registered agents остаются доступными                   |
| Queue overload                  | Явный `ResourceExhausted`, нет OOM/unbounded latency           |

---

### 21.15 Definition of Done для production readiness

Remote adapter не считается production-ready, пока не выполнены все пункты:

- bounded queues, buffers и connection pools;
- per-agent/per-caller/global limits;
- durable critical event journal;
- SSE reconnect/replay с cursor и pagination;
- slow-consumer isolation;
- event progress coalescing;
- artifact store без in-memory buffering больших файлов;
- all timeout classes;
- idempotency и safe retry/fallback rules;
- structured logs, metrics, tracing и alerts;
- graceful shutdown/recovery tests;
- load и fault-injection tests из раздела 21.14.
