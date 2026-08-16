# Спека модуля `driver-mcp` — MCP client backend для agent-connector

## Обновление к предыдущему анализу

Проверка спецификации MCP 2026-07-28 показала, что базовый протокол **богаче**, чем я предполагал ранее в этой сессии: он включает `notifications/progress` через `progressToken` [web:353][web:354], server-side `notifications/tools/list_changed` [web:341][web:354], и experimental `InputRequiredResult` для multi-turn tool calls [web:341]. Это означает `DriverCapabilities` для MCP backend не обязана быть "streaming: false, cancellation: false, provide_input: false" по умолчанию — способности зависят от того, что конкретный MCP-сервер реально объявляет при `initialize` capability negotiation, и это нужно проверять per-server, не считать общим правилом для всего MCP.

Официальный Rust SDK — `rmcp` (crates.io, `github.com/modelcontextprotocol/rust-sdk`), поддерживает stdio/HTTP streaming/WebSocket, версия 0.8.0 на момент проверки, dev channel через git branch `main`. [web:351] Это рекомендуемая зависимость для `driver-mcp`, а не одна из альтернативных сторонних реализаций (`mcp_rust_sdk`, `mcpx`, `pmcp`), потому что она официальная и поддерживается организацией-держателем спецификации.

## 1. Цель модуля

`driver-mcp` — новый crate в workspace `agent-connector`, реализующий существующий `AgentDriver` trait (см. `adapter-core`) поверх MCP client-соединения к внешнему MCP-серверу. Модуль транслирует MCP tools в ACP/A2A-совместимые skills, а MCP tool calls — в существующий `InvokeRequest → DriverEvent` контракт `AdapterCore`. Модуль не меняет `adapter-core`, `adapter-model` или протокольные mapper'ы — это чисто новый backend, симметричный `driver-stdio`/`driver-http-sse`.

## 2. Место в архитектуре

```text
                 A2A client / ACP client
                          │
                    adapter-core
       task lifecycle | registry | policy | scheduler
                          │
              ┌───────────┼───────────────┐
         driver-stdio  driver-http-sse  driver-mcp      ← новый модуль
              │              │              │
         local agent    remote agent    MCP server(s)
                                        (rmcp client, stdio/HTTP/WS)
```

`driver-mcp` зависит от `adapter-core` (для trait `AgentDriver`) и `adapter-model` (для DTO), плюс `rmcp` как MCP client SDK. Он не зависит от `protocol-a2a-*`/`protocol-acp-*` — модуль работает исключительно на уровне driver, ниже protocol layer.

## 3. Зависимости

```toml
[dependencies]
adapter-core = { path = "../adapter-core" }
adapter-model = { path = "../adapter-model" }
rmcp = { version = "0.8", features = ["client"] }
tokio = { workspace = true }
async-trait = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
```

## 4. Конфигурация

Новый вариант `AgentTransportConfig` в `adapterd-config` (см. `crates/adapterd/src/config.rs`), симметричный существующим `Stdio`/`HttpSse`:

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "driver", rename_all = "kebab-case")]
pub enum AgentTransportConfig {
    Stdio { /* существующее */ },
    HttpSse { /* существующее */ },
    Mcp {
        /// Транспорт до самого MCP-сервера: как rmcp соединяется с ним.
        #[serde(flatten)]
        transport: McpTransportConfig,
        /// Явный allowlist tool names, которые должны стать skills. Пусто =
        /// все tools сервера (не рекомендуется для remote/public profile —
        /// см. раздел безопасности).
        #[serde(default)]
        allowed_tools: Vec<String>,
        /// Таймаут на discovery (tools/list) при старте.
        #[serde(default = "default_mcp_discovery_timeout")]
        discovery_timeout_seconds: u64,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "mcp_transport", rename_all = "kebab-case")]
pub enum McpTransportConfig {
    Stdio {
        command: PathBuf,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    Http {
        endpoint: String,
        #[serde(default)]
        token_env: Option<String>,
        #[serde(default)]
        allow_http_development: bool,
    },
}

fn default_mcp_discovery_timeout() -> u64 { 10 }
```

Пример `adapter.yaml`:

```yaml
agents:
  - id: search-mcp
    skills: []                 # заполнится автоматически из allowed_tools при старте
    driver: mcp
    mcp_transport: stdio
    command: ./mcp-servers/search-server
    allowed_tools: ["web_search", "fetch_page"]
    discovery_timeout_seconds: 10
    limits:
      max_concurrent_tasks: 4
      default_timeout_seconds: 60
```

`allowed_tools` — обязательный allowlist в remote/production profile. Пустой список (разрешить все tools сервера) допустим только в local/dev profile — это симметрично правилу для public demo из вашей памяти: "отвергать произвольные внешние agent endpoints", применённому здесь к tools вместо endpoints. [memory:1]

## 5. Discovery: MCP tools → skills

При старте `McpDriver::new()`:

1. Устанавливает MCP-соединение через `rmcp` (stdio spawn или HTTP connect, в зависимости от `McpTransportConfig`).
2. Выполняет `initialize` handshake, читает capability negotiation результат сервера (`tools`, `tools.listChanged`, поддержка `progressToken` и т.д. — сохраняется в `McpServerCapabilities` внутри driver).
3. Вызывает `tools/list` (с пагинацией, если сервер её использует). [web:341]
4. Фильтрует результат через `allowed_tools`, если задан; иначе берёт все.
5. Каждый оставшийся tool превращается в skill: `skill_id = tool.name`, JSON schema tool'а сохраняется для последующей валидации input при `invoke()`.
6. Если discovery не завершился в `discovery_timeout_seconds` — driver считается unhealthy, `RegisteredAgent` не регистрируется, `adapterd` логирует явную ошибку при старте (не паникует весь процесс, если это один из нескольких агентов — остальные продолжают регистрацию).

Если сервер объявил `tools.listChanged: true`, driver подписывается на `subscriptions/listen` с `toolsListChanged: true` [web:343] и обновляет внутренний skill-список на лету — но **не** меняет уже зарегистрированные `RegisteredAgent.skills` в реальном времени в этой версии (см. раздел 9, "Известные ограничения").

## 6. Маппинг `AgentDriver` trait → MCP операции

```rust
pub struct McpDriver {
    id: String,
    client: rmcp::Client,             // тип условный, см. точный API rmcp 0.8
    server_capabilities: McpServerCapabilities,
    tool_schemas: HashMap<String, serde_json::Value>, // skill_id -> inputSchema
    default_timeout: Duration,
}

#[derive(Clone, Debug, Default)]
struct McpServerCapabilities {
    supports_progress: bool,
    supports_cancellation: bool,
    supports_input_required: bool,   // experimental SEP, см. раздел 7
}
```

### `id()` / `capabilities()`

`capabilities()` возвращает `DriverCapabilities`, **производную от фактического `McpServerCapabilities`**, не хардкод:

```rust
fn capabilities(&self) -> DriverCapabilities {
    DriverCapabilities {
        streaming: self.server_capabilities.supports_progress,
        cancellation: self.server_capabilities.supports_cancellation,
        provide_input: self.server_capabilities.supports_input_required,
    }
}
```

### `health()`

Лёгкий MCP `ping` (если сервер поддерживает) или повторный `tools/list` с коротким таймаутом как fallback. Не тяжелее, чем health-check других driver'ов.

### `invoke(task_id, request) -> Result<mpsc::Receiver<DriverEvent>, CoreError>`

1. Резолвит `skill_id` из `request` в MCP tool name (уже проверено на этапе `AgentRegistry::resolve`, здесь просто lookup schema).
2. Транслирует `request.input: Vec<Part>` в MCP `arguments` JSON-объект **по input schema конкретного tool** — не универсальным способом:
   - `Part::Text { text }` → если schema ожидает конкретное поле строкового типа, кладёт туда; если schema — произвольный объект, использует конвенцию (например, `{"input": text}` или конфигурируемый маппинг per-tool в будущей версии).
   - `Part::Json { value }` → напрямую как `arguments`, если `value` уже объект; иначе оборачивает.
   - `Part::FileRef { uri, mime_type }` → в MCP resource reference формат, если tool ожидает resource-параметр; иначе явная ошибка "tool does not accept file input".
3. Если `self.server_capabilities.supports_progress`, добавляет `_meta.progressToken` в запрос [web:353] и подписывается на `notifications/progress` на response stream этого конкретного request — не на общий `subscriptions/listen` (progress — request-scoped, не глобальная подписка [web:343]).
4. Отправляет `tools/call`.
5. Транслирует поток событий в `DriverEvent`:
   - `notifications/progress` → `DriverEvent::Progress { message, percent }`.
   - Финальный `tools/call` result с `isError: false` → `DriverEvent::Completed(parts)`, где `parts` строится из `content: ContentBlock[]` результата (text/resource/image блоки → `Part::Text`/`Part::FileRef`/`Part::Json`).
   - Финальный result с `isError: true` → `DriverEvent::Failed(PublicError { message: <из content>, retryable: false })`.
   - `InputRequiredResult` (если сервер поддерживает experimental SEP) → `DriverEvent::InputRequired(InputRequest { question: <из результата> })`, task переходит в `WaitingForInput` — то же самое, что для stdio/http-sse driver.
6. Возвращает `mpsc::Receiver<DriverEvent>`, как и другие driver'ы.

### `cancel(task_id) -> Result<(), CoreError>`

Если `self.server_capabilities.supports_cancellation` — отправляет MCP cancellation notification для соответствующего request ID. Если сервер не объявил эту capability — возвращает `Ok(())` как no-op **с явным `tracing::warn!`**, не molчаливый успех: `AdapterCore::cancel()` уже проверяет `agent.driver.capabilities().cancellation` перед вызовом `driver.cancel()` (это подтверждено чтением реального кода `adapter-core/src/lib.rs`), так что если `capabilities()` честно возвращает `false`, `cancel()` на driver вообще не будет вызван в нормальном потоке — эта ветка защитная, на случай прямого вызова.

### `provide_input(task_id, input) -> Result<(), CoreError>`

Если `self.server_capabilities.supports_input_required` — отправляет continuation запрос по experimental SEP-1686/`InputRequiredResult` паттерну [web:341][web:350] (точный wire format зависит от финальной ратификации SEP на момент реализации — **это открытый риск**, см. раздел 9). Если сервер не поддерживает — `Err(CoreError::InvalidRequest("MCP server does not support mid-call input"))`, симметрично существующей проверке в `adapter-core::provide_input()`.

## 7. Известные протокольные риски

**`InputRequiredResult`/SEP-1686 — experimental, не финализированная часть спеки** на момент проверки (помечена как SEP, не как ratified core spec) [web:350]. Реализация multi-turn через этот путь должна быть feature-gated и явно помечена как experimental в конфиге (`allow_experimental_input_required: bool`, default `false`), чтобы не сломаться при изменении SEP до финализации.

**`tasks/list`/task primitive (SEP-1686)** — отдельная от `tools/call` концепция долгоживущих задач в MCP, тоже experimental [web:350]. Не путать с ACP/A2A task lifecycle — если сервер поддерживает MCP task primitive, это отдельный, более близкий к ACP аналог, который стоит рассмотреть как альтернативный/дополнительный маппинг в будущей версии `driver-mcp`, но не в MVP.

**Версионирование протокола.** MCP 2026-07-28 меняет resource subscription модель (`subscriptions/listen` вместо `resources/subscribe`/`unsubscribe`) [web:343]. `driver-mcp` должен явно проверять версию протокола сервера при `initialize` и поддерживать хотя бы одну стабильную версию назад (2025-11-25, судя по заявленной совместимости `rmcp` [web:351]), не полагаться только на самую новую.

## 8. Безопасность

Прямое применение canonical принципа secure-by-profile и вашего правила для public demo:

- **Local profile**: MCP stdio-сервер как child-процесс — то же самое, что уже есть для `driver-stdio` (subprocess ownership boundary), никаких дополнительных требований.
- **Remote profile**: MCP HTTP-транспорт требует TLS (`allow_http_development` flag, симметричный `driver-http-sse`), bearer token из `token_env`.
- **`allowed_tools` allowlist обязателен вне local/dev profile** — сервер может добавить новый опасный tool в любой момент (`tools/list_changed`), и без allowlist это автоматически станет вызываемым skill без ревью. Это прямая аналогия правилу "не принимать произвольные external agent endpoints" из вашей публичной demo-политики. [memory:1]
- **Input schema validation до отправки MCP call**: `driver-mcp` должен валидировать `request.input` против сохранённой `inputSchema` **на своей стороне**, прежде чем отправлять `tools/call` — не полагаться только на серверную валидацию, чтобы дать быстрый и понятный `CoreError::InvalidRequest` вместо непрозрачной MCP-level ошибки.
- **Не проксировать MCP resource URIs напрямую без проверки** — если tool возвращает `resource` content block с произвольным URI, `driver-mcp` не должен автоматически делать fetch этого URI на стороне adapter-connector без явного capability/allowlist — тот же SSRF-риск, что canonical документ уже упоминает для transport discovery manifests.

## 9. Известные ограничения MVP

- Динамическое обновление `RegisteredAgent.skills` при `notifications/tools/list_changed` не реализуется в первой версии — обновление skills требует restart driver (`adapterd` restart агента через существующий lifecycle, не hot-reload). Это явный, документированный trade-off, не забытая фича.
- Multi-turn через `InputRequiredResult` — experimental, за отдельным флагом, не включено по умолчанию.
- MCP `prompts`/`resources` primitives (отдельные от `tools`) не маппятся в этой версии — `driver-mcp` работает только с `tools` capability. Если нужен доступ к MCP resources как часть агентского контекста, это отдельная будущая задача, не часть MVP driver.
- Пагинация `tools/list` поддерживается при discovery, но re-discovery по `list_changed` notification (если реализовано) должен корректно обрабатывать multi-page ответы так же.

## 10. Тесты

- Unit: маппинг `Part` ↔ MCP `arguments`/`content` для каждого типа `Part`, включая edge cases (schema не совпадает с ожидаемым форматом — явная ошибка, не silent best-effort).
- Unit: `DriverCapabilities` корректно производится из mock `McpServerCapabilities` для всех комбинаций (progress/no-progress, cancellation/no-cancellation).
- Integration: реальный MCP stdio test-server (можно взять минимальный echo-tool сервер из `rmcp` examples) — full round-trip `invoke()` → `Completed`.
- Integration: cancellation path с mock-сервером, поддерживающим и не поддерживающим `notifications/cancelled`.
- Security test: `allowed_tools` allowlist действительно блокирует незаявленные tools, даже если сервер их возвращает в `tools/list`.

## 11. Definition of done

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Плюс в PR: подтверждение версии MCP spec, с которой протестирован модуль; явный список experimental features, включённых/выключенных по умолчанию; результат integration-теста против хотя бы одного реального MCP-сервера (не только mock).
