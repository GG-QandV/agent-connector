# Streaming Architecture — agent-connector / ACP-A2A Gateway

## 1. Цель

Единая, протокол-независимая модель доставки событий задачи (`CoreEvent`) от драйвера агента до клиента (A2A SSE, ACP JSON-RPC, будущие транспорты), гарантирующая:

- отсутствие потери событий при lag, reconnect, race и переполнении канала;
- идемпотентное восстановление по `seq` / `after_seq`;
- единую точку правды для истории задачи (durable store) и live-доставки (`broadcast`).

Основано на анализе `crates/adapter-core`, `crates/protocol-a2a-mapper`, `crates/protocol-acp-mapper`, `crates/protocol-acp-runtime`, `crates/driver-http-sse` репозитория `GG-QandV/agent-connector`. [file:51][file:53][file:54][github_mcp_direct:1][github_mcp_direct:2]

## 2. Текущая схема (as-is)

```text
Driver / Agent
      │
      │ CoreEvent { seq, task_id, kind }
      ▼
AdapterCore
      ├── durable task history   (SQLite / Postgres task-store)
      └── broadcast::Sender<CoreEvent>   (capacity = 256)
                    │
                    ▼
       TaskSubscription { history: Vec<CoreEvent>, receiver }
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
   protocol-a2a-mapper   protocol-acp-mapper
          │                   │
          ▼                   ▼
     A2A SSE (HTTP)     ACP stdio JSON-RPC
                              │
                              ▼
                    session/update (pull, polling)
```

Компоненты:

- **`AdapterCore`** — источник истины по task lifecycle, хранит durable history и публикует live-события через `tokio::sync::broadcast::channel(256)`.
- **`TaskSubscription`** — пара `(history, broadcast::Receiver<CoreEvent>)`, возвращается `core.subscribe(task_id, after_seq)`.
- **`protocol-a2a-mapper`** — отдаёт `history` + `receiver.recv()` как единый `A2aTaskEventStream`, транслируя `CoreEventKind` в `A2aStreamEvent` (`TaskStatusUpdate` / `TaskArtifactUpdate`).
- **`protocol-acp-mapper` / `protocol-acp-runtime`** — сейчас реализует только **pull**-модель: `session/update` отдаёт снапшот истории, live push отсутствует, хотя `capabilities.streaming = true` заявлен.
- **`driver-http-sse`** — исходящий (agent→adapter) транспорт: SSE-клиент с `after_seq`, `Last-Event-ID`, reconnect backoff, timeouts, `max_event_bytes`.

## 3. Инварианты (must-hold)

1. **Durable-first**: ни одно терминальное или структурное событие (`Accepted`, `InputRequired`, `Artifact`, `Completed`, `Failed`, `Cancelled`) не может быть потеряно — только `broadcast` может "лагать", durable store — никогда.
2. **Монотонность `seq`**: клиент никогда не получает событие с `seq <= after_seq`, который он уже подтвердил.
3. **Явный Lagged, не тихий drop**: `broadcast::error::RecvError::Lagged(n)` обязателен к обработке через durable catch-up, а не проброс наверх без recovery. Подтверждено тестом `broadcast_overflow.rs`. [file:9]
4. **Без гонки history↔receiver**: receiver обязан открываться **до** чтения history (или до фиксации watermark), иначе возможно окно потери события между двумя операциями.
5. **Единый writer на транспорт**: для stdio (ACP) один writer task на весь процесс — нельзя писать в stdout из нескольких параллельных задач конкурентно.
6. **Terminal event = конец потока**: после `Completed` / `Failed` / `Cancelled` reconnect транспортного слоя не производится.

## 4. Целевая схема (to-be) — `ReliableTaskStream`

Вводится единый слой в `adapter-core`, который инкапсулирует recovery-логику один раз, а не дублирует её в каждом mapper'е.

```text
                     AdapterCore::subscribe(task_id, after_seq)
                                   │
                 ┌─────────────────┴─────────────────┐
                 │   1. open broadcast::Receiver       │
                 │   2. read durable history > after_seq│
                 │   3. filter events already in live   │
                 │      buffer (dedupe by seq)           │
                 └─────────────────┬─────────────────┘
                                   ▼
                     ReliableTaskStream {
                         task_id,
                         after_seq,
                         pending: VecDeque<CoreEvent>,
                         receiver: broadcast::Receiver<CoreEvent>,
                     }
                                   │
                     .next() -> Option<CoreEvent>
                                   │
        ┌───────────────┬─────────┴─────────┬───────────────┐
        ▼               ▼                   ▼               ▼
   A2A SSE writer   ACP push writer   HTTP/SSE driver   (future: WS)
```

### 4.1 Алгоритм `ReliableTaskStream::next()`

```text
1. Если pending непусто → pop_front() и вернуть.
2. recv() из broadcast.
3. Ok(event):
     если event.seq <= after_seq → пропустить, повторить.
     если event.seq == after_seq + 1 → after_seq = seq; вернуть событие.
     если event.seq > after_seq + 1 (gap) → durable catch-up(after_seq..event.seq),
         положить event в конец pending, вернуть первый catch-up event.
4. Err(Lagged(_)):
     durable catch-up(after_seq..), заполнить pending, вернуть первый.
5. Err(Closed):
     вернуть None (поток завершён).
6. После доставки terminal-события (Completed/Failed/Cancelled) —
   пометить stream как завершённый, дальнейшие next() -> None.
```

### 4.2 Контракт `after_seq`

`after_seq` — единственная точка синхронизации между клиентом и сервером: последний **подтверждённый клиентом** seq. Все остальные идентификаторы транспорта (`Last-Event-ID`, `revision`) преобразуются в `after_seq` на границе транспорта, не проникая во внутренние структуры.

## 5. A2A (HTTP/SSE) — транспортный контракт

```text
GET /v1/a2a/tasks/{task_id}/events?after_seq=N
Accept: text/event-stream

id: 42
event: task.status
data: {"taskId":"...","seq":42,"state":"working"}

id: 43
event: task.artifact
data: {"taskId":"...","seq":43,"artifact":{...}}

: keepalive

id: 47
event: task.status
data: {"taskId":"...","seq":47,"state":"completed","final":true}
```

Требования:

- каждый SSE-фрейм несёт `id: <seq>`;
- heartbeat/comment каждые 15–30с против proxy-таймаутов;
- stream закрывается только после terminal `final:true`;
- при разрыве соединения клиент переподключается с `Last-Event-ID` = последний обработанный `seq`.

## 6. HTTP/SSE driver (agent → adapter, исходящий клиент) — исправления контракта

- `manifest` должен быть `Arc<RwLock<Option<UaicManifest>>>`, а не пересоздаваться в `clone_for_task()` — иначе теряется кэш между task-scoped клоном и spawned stream task. [file:51]
- `max_event_bytes` должен считаться **на кадр (frame)**, а не на весь накопительный buffer — иначе несколько мелких событий в одном TCP chunk ложно триггерят лимит.
- backoff должен сбрасываться после первого успешно обработанного события в новой сессии, а не расти монотонно между несвязанными сбоями.
- различать `ConsumerClosed` (локальный receiver закрыт — не reconnect'иться) от `TransportFailure` (сетевая ошибка — reconnect с backoff) и `Terminal` (конец задачи — остановка навсегда).
- SSE parser — заменить самодельный `find_sse_boundary`/`parse_sse_frame` на полноценный decoder с поддержкой `event:`, `retry:`, multi-line `data:`, comment-строк (`:`).

## 7. ACP — выбор модели

Текущее состояние: `capabilities.streaming = true`, но `session/update` — chistый pull/poll без push. [file:52] Нужно явно выбрать одну модель.

### Вариант A — Pull (MVP, минимальный риск)

```text
session/prompt  → { sessionId, taskId }
session/update(taskId, afterSeq) → { events: [...после afterSeq...] }
```

- `capabilities.streaming = false` (честно отражает реальность);
- `session/update` обязателен принимать `afterSeq`, дедуплицирует через `ReliableTaskStream`-catch-up внутри runtime (Lagged скрыт от клиента);
- клиент сам управляет частотой поллинга.

### Вариант B — Push (полноценный streaming)

```text
session/prompt → { sessionId, taskId }
                       │
             background subscription task
                       │  ReliableTaskStream::next()
                       ▼
        mpsc::Sender<JsonRpcNotification>  (per-process, единый)
                       │
                       ▼
              single stdout writer task
                       │
                       ▼
   → notification "session/update" { taskId, seq, update }
```

Обязательное условие: все параллельные task-streams мультиплексируются через **один** `mpsc`-канал в **единственный** writer task, чтобы избежать перемешивания JSON-RPC строк в stdout.

## 8. Backpressure — политика по слоям

| Слой | Механизм | Поведение при перегрузке |
|---|---|---|
| Durable store | SQLite/Postgres append-only | Никогда не теряет события |
| `broadcast::channel(256)` | Ring-buffer в памяти | Допускает `Lagged`, требует durable catch-up |
| `A2aTaskEventStream` / ACP mapper | `ReliableTaskStream` (новый слой) | Скрывает `Lagged` от клиента через recovery |
| SSE writer (сервер → HTTP клиент) | Async write to TCP | Не блокирует producer навсегда; таймаут slow-client |
| `mpsc::channel(128)` (HTTP driver) | Bounded channel | Consumer closed → останов, не reconnect |
| ACP stdout writer | Bounded `mpsc` + single writer | Backpressure на уровне очереди уведомлений |

Допустимо **coalescing** только для `Progress` (можно схлопнуть промежуточные проценты при явном отставании клиента). Запрещено coalescing для `Accepted`, `InputRequired`, `Artifact`, `Completed`, `Failed`, `Cancelled` — эти события должны доставляться по одному и в полном составе.

## 9. Известные семантические дефекты, влияющие на streaming

- `protocol-acp-mapper::map_event` подставляет `session_id = "unknown"` — ломает сопоставление update↔session при reconnect. [file:53]
- `InputRequired` генерирует новый `request_id = Uuid::new_v4()` при каждом маппинге события — при повторной доставке (после Lagged/reconnect) ID будет другим. `request_id` должен быть частью `CoreEvent`/`InputRequest`, а не генерироваться на границе mapper'а. [file:53]
- `A2aMapper::send_task` / `get_task` всегда возвращают `artifacts: Vec::new()` — несогласованность между snapshot-ответом и потоком artifact-событий. [file:54]

## 10. Итоговая диаграмма целевого состояния

```text
                         ┌───────────────────────┐
                         │      AdapterCore        │
                         │  durable history + seq  │
                         │  broadcast::channel(256) │
                         └───────────┬─────────────┘
                                     │ subscribe(task_id, after_seq)
                                     ▼
                         ┌───────────────────────┐
                         │   ReliableTaskStream    │
                         │ (gap/lag/dup handling)  │
                         └───────────┬─────────────┘
              ┌──────────────────────┼──────────────────────┐
              ▼                      ▼                      ▼
     A2A SSE endpoint        ACP push/pull runtime     driver-http-sse
     (id:, heartbeat,        (single writer,           (Arc manifest,
      terminal close)         afterSeq contract)         frame-based limit,
                                                          reset backoff)
```
