# ANP Phase 3 — Что нужно получить от агента / upstream

## Цель Phase 3

Реализовать `driver-anp-client`: `agent-connector` должен вызывать удалённого ANP peer как обычный `AgentDriver`.

```text
AdapterCore
  → driver-anp-client
  → AnpTransport (SDK adapter)
  → ANP peer
```

`AdapterCore` остаётся единственным владельцем local `TaskId`, lifecycle, durable events и downstream streaming.

## Обязательный пакет данных

Перед реализацией real SDK adapter агент должен дать точные, проверяемые ответы с ссылками на spec/commit/исходный код.

### 1. Версия и Rust package

- Точная upstream ссылка и immutable commit SHA.
- Cargo package name, crate features и MSRV.
- Минимальный `Cargo.toml` example подключения.
- License и статус стабильности API.
- Команда, которая собирает minimal example (`cargo test`/`cargo run`).

### 2. Peer discovery и identity

- Как получить endpoint peer: static URL, DID resolution, WNS или другой механизм.
- Формат peer ID/DID и DID document.
- Как проверить связь `DID → endpoint → verification key`.
- Какие алгоритмы/keys используются.
- Как выполняется rotation/revocation ключа.
- Готовый тестовый DID/endpoint либо локальный reference peer.
- Какая trust model допустима: pinned DID/key, CA/root of trust; TOFU для production не принимается.

### 3. Канал и аутентификация

- Конкретный transport: HTTP, WebSocket, libp2p, direct E2EE или иной.
- Как клиент создаёт authenticated/encrypted channel.
- Какие request headers, signatures, nonces, timestamps и replay protections обязательны.
- Где и как хранятся private keys/credentials.
- Какие ошибки возвращаются при invalid proof/auth/expired key.

### 4. Task operations

Для каждой операции нужны exact Rust calls или wire examples request/response:

| Операция | Обязательные данные |
|---|---|
| Invoke | remote agent/skill, input, context, deadline, idempotency key, response с remote task ID |
| Status | запрос статуса по remote task ID, terminal/non-terminal states |
| Cancel | idempotent cancel, возможные response states |
| Provide input | stable input/request ID и формат ответа |
| Capabilities | streaming, resume, cancellation, input, artifacts, protocol version |

Нужно явно ответить: **при повторном `invoke` с тем же idempotency key создаётся новая remote task или возвращается существующая?**

### 5. Event stream и reconnect

Это блокер для claims о reliable streaming.

Агент должен предоставить:

- Как открыть stream для конкретного remote task.
- Реальный event envelope: `task_id`, event type, payload, `event_id`, `seq`/cursor.
- Гарантирован ли `seq` строго монотонным в рамках task.
- Reconnect API: `after_seq`, `Last-Event-ID`, cursor token или другой explicit resume cursor.
- Контракт: reconnect возвращает все события **строго после cursor**, затем live stream.
- Что происходит при duplicate, event gap, server restart, expired history.
- Retention истории и явная ошибка, если resume больше невозможен.
- Как обозначаются `Completed`, `Failed`, `Cancelled` и когда stream закрывается.
- Keepalive/idle timeout semantics.

Минимальный required example:

```text
invoke → remote_task_id
stream → seq=1 accepted
stream → seq=2 progress
<network disconnect>
stream(after_seq=2) → seq=3 artifact
stream → seq=4 completed
```

Если upstream не даёт stable cursor + catch-up history, driver будет отмечать поток как **non-resumable**: reconnect не сможет обещать отсутствие потерь.

### 6. Error taxonomy

Нужна таблица upstream errors → категория:

```text
identity_untrusted | authorization | unsupported_capability |
rate_limited | transport_failure | protocol_failure |
stream_gap | resume_unavailable | remote_task_not_found
```

Для каждой ошибки: retryable ли она, безопасен ли retry invoke, есть ли retry-after.

## Формат результата от агента

Агент должен вернуть один Markdown-документ со следующими разделами:

```text
1. Upstream commit and Rust crate
2. Runnable minimal Rust client
3. Local/test peer setup
4. Discovery and DID trust flow
5. Invoke/status/cancel/input API
6. Stream event schema and resume contract
7. Error/retry table
8. Capability/version negotiation
9. Security/key-management constraints
10. Known gaps versus P0 spec
```

И приложить:

- ссылки на точные upstream paths/lines;
- один working invoke example;
- один working reconnect/resume example;
- тестовые ключи только для local fixture, никогда для production.

## Критерий готовности к real SDK adapter

Переходим от `AnpTransport` mock к реальному SDK adapter только если одновременно подтверждены:

- immutable SDK revision и воспроизводимая сборка;
- verified peer identity;
- idempotent invoke;
- cancel и status;
- stream с stable cursor;
- documented resume/catch-up semantics;
- independent local or remote ANP peer для integration tests.
