# Спецификация: ACP stdio-режим в `adapterd`

## Модель

Один бинарник `adapterd`, один процесс. Оба wire-транспорта (A2A HTTP, ACP stdio) — независимые listener-задачи внутри одного `tokio` runtime, разделяющие один `Arc<AdapterCore>`. Включаются/выключаются через config, не через отдельные бинарники.

```text
adapterd::main()
├── build Config, TaskStore, AgentRegistry, AdapterCore  (общее для обоих транспортов)
├── if config.servers.a2a_http.enabled  → spawn A2A HTTP listener (Axum, bind TCP)
├── if config.servers.acp_stdio.enabled → spawn ACP stdio loop (читает stdin/stdout процесса)
├── spawn retention cleanup
└── ждать shutdown signal → draining → grace period → закрыть оба транспорта
```

## Инвариант взаимоисключения (практическая рекомендация, не hard-constraint)

Технически оба транспорта могут быть `enabled: true` одновременно — они не конфликтуют на уровне ресурсов. Но в реальном деплое это почти всегда антипаттерн:

- ACP stdio предполагает, что процесс порождён родителем (IDE, host-agent), который **сам владеет** stdin/stdout этого процесса и общается через pipes.
- Если тот же процесс параллельно поднимает HTTP-сервер, это создаёт скрытый лишний attack surface для сценария, где stdio — единственно ожидаемый канал.

Рекомендация: логировать warning при старте, если оба enabled одновременно, но не блокировать — экземпляр «connector as both a local subprocess and a remote HTTP peer» теоретически валиден для тестовых/dev-сценариев.

## Конфигурация

```yaml
servers:
  a2a_http:
    enabled: true
    bind: "127.0.0.1:8080"
    public_base_url: "https://connector.example.com"
  acp_stdio:
    enabled: false
    max_line_bytes: 1048576
    shutdown_grace_seconds: 5
    agent_name: "agent-connector"
    agent_version: "0.1.0"
```

Если `acp_stdio.enabled: true`, `adapterd` при старте читает `stdin`/пишет `stdout` **своего собственного процесса** — то есть тот, кто запускает `adapterd` в этом режиме, должен запускать его как child process с захваченными pipes (аналогично тому, как IDE запускает LSP-серверы). Логи в этом режиме обязаны идти **только** в stderr (уже реализовано в `protocol-acp-runtime` через `tracing`), иначе они испортят ACP JSON-RPC поток на stdout.

## Поведение при старте

1. Прочитать и провалидировать `Config`.
2. Построить `TaskStore`, `AgentRegistry`, `PolicyEngine`, `AdapterCore` — идентично для обоих транспортов, никакого дублирования domain-логики.
3. Если `a2a_http.enabled`: собрать Axum router (`agent_card_router + jsonrpc_router + health_router`), забиндить TCP listener, запустить `axum::serve` в отдельной `tokio::spawn`-задаче.
4. Если `acp_stdio.enabled`: построить `AcpRuntime` над `tokio::io::stdin()`/`tokio::io::stdout()`, запустить `run_with_shutdown(shutdown_token)` в отдельной `tokio::spawn`-задаче.
5. Если ни один транспорт не enabled — hard error при старте, процесс не имеет смысла без хотя бы одного способа принимать задачи.
6. Дождаться сигнала остановки (`SIGINT`/`SIGTERM` или общий `CancellationToken`).

## Поведение при остановке

1. Получен shutdown signal → выставить общий `draining: Arc<AtomicBool>` в `true`.
2. `/readyz` немедленно начинает отвечать `503` (уже реализовано в `health.rs`).
3. ACP stdio: `drain_token.cancel()` → новые top-level requests получают explicit `"server is shutting down"` ответ, in-flight операции ждут завершения в пределах `shutdown_grace_seconds`.
4. A2A HTTP: Axum graceful shutdown — новые соединения не принимаются, in-flight запросы завершаются в пределах общего `shutdown_grace_seconds` daemon-уровня.
5. После истечения grace period или завершения обоих транспортов — остановить cleanup-worker, выйти с кодом `0`.
6. Если ACP stdio получает `EOF` на stdin **раньше** внешнего shutdown-сигнала (родительский процесс закрыл pipe) — это самостоятельный триггер на завершение **всего** `adapterd`, а не только ACP-части, потому что если stdio был единственным транспортом, процесс больше не имеет смысла жить. Если A2A HTTP тоже enabled — процесс может продолжать жить только через HTTP, завершив только ACP-задачу.

## Публичный контракт для внешнего наблюдателя

- Если `acp_stdio.enabled: true`, любой байт, записанный в stdout процесса **кроме** валидных ACP JSON-RPC строк — это баг, независимо от того, что происходит в A2A/HTTP части.
- Если оба транспорта включены и оба обслуживают одни и те же зарегистрированные агенты, `AdapterCore` гарантирует единый task lifecycle — задача, созданная через один транспорт, видна через другой (например, `session/get` по ACP может увидеть task, созданный через A2A `message/send`, если у task есть общий task_id/idempotency namespace — это уже свойство `AdapterCore`, не специфично для транспорта).
