# Аутентификация A2A HTTP слоя — подтверждено кодом SDK

## Цепочка end-to-end

```text
HTTP request
  → axum::HeaderMap
  → extract_service_params(headers)          [a2a-server/src/middleware.rs:17]
      lowercase keys, Vec<String> values, insertion order, non-ASCII skipped
  → ServiceParams = HashMap<String, Vec<String>>   [middleware.rs:10]
  → передаётся ПЕРВЫМ аргументом в КАЖДЫЙ RequestHandler метод
      (send_message, get_task, cancel_task, subscribe_to_task, ...)
      [jsonrpc.rs:44-49, handle_jsonrpc]
  → params["authorization"] = ["Bearer <token>"]
  → BearerTokenPolicy::resolve(token) → Caller
```

Подтверждено тестами самого SDK: `"authorization" → params["authorization"] = ["Bearer jsonrpc-token"]` — буквальный контракт pinned commit `02ee5602...`, а не наша интерпретация.

## Архитектурное решение

**`resolve_caller()` в `executor.rs` — правильное место и правильный механизм.** Переопределение `RequestHandler` trait не требуется. Side-channel `DashMap<TaskId, Caller>` не требуется. `CallContext`/`CallInterceptor` из SDK не используются: `ServiceParams` уже даёт всё напрямую.

Путь данных:

```text
params: &ServiceParams (в RequestHandler методе)
  → SDK кладёт service_params: params.clone() в ExecutorContext
  → ctx.service_params доступен внутри AgentExecutor::execute(ctx)
  → resolve_caller(&ctx.service_params) → Caller
```

`BearerTokenPolicy` (фикс 2) остаётся верным как есть; обёртка над `RequestHandler` была избыточным усложнением.

## Чеклист для `resolve_caller()` — статус по фактам

Три пункта закрыты контрактом SDK:

- ✅ Регистр ключа — lowercase, `params.get("authorization")` корректен.
- ✅ Тип значения — `Vec<String>`, нужен явный `.first()`.
- ✅ Откуда router строит `ServiceParams` — `extract_service_params(&headers)` в `jsonrpc.rs:44-49`, вызывается для каждого запроса.
- ✅ Отсутствие header vs невалидный токен — в нашем `auth.rs` оба случая дают единообразный 401 (`require_bearer_auth`), в `executor.rs` — `invalid_request` с разными сообщениями. Статус не раскрывает причину.
- ✅ Пустой токен после `"Bearer "` — `strip_prefix("Bearer ")` на `"Bearer "` даёт `Some("")` → `policy.resolve("")` → `None` → 401 (пустого токена в grants нет).
- ✅ REST-биндинг — в `crates/protocol-a2a-server` нет `rest.rs`, используется только `jsonrpc_router` → auth работает на всём inbound.

Остались осознанные решения (см. `docs/operations.md` → "Bearer token parsing edge cases"):

1. **Множественные `Authorization` headers** — `resolve_caller()` берёт `.first()` молча; решение должно быть осознанным (первый/отклонять/последний), не случайным.
2. **Точный префикс `"Bearer "`** — `strip_prefix("Bearer ")` case-sensitive с ровно одним пробелом; решить явно, разрешать ли lowercase `"bearer"`.
3. **Пустой токен** — подтверждено, что даёт 401, но стоит добавить явную проверку для ясности.

## Статус auth-фиксов

| Фикс | Статус |
|---|---|
| Concurrency: per-caller quota | Не зависит от auth, остаётся в силе |
| Auth: `BearerTokenPolicy` + resolve из headers | Подтверждён механизм передачи (`ServiceParams` → `ExecutorContext.service_params`) |
| Subscribe race: canonical history-first | Подтверждён canonical-документом и `subscription_stream()` в самом SDK |
| ACP: `shutdown_grace` wiring | Не связан с auth, остаётся в силе |
| Auth: per-request caller через `ServiceParams` | Закрыт полностью; обёртка над `RequestHandler` отозвана как избыточная |