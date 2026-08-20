//! DIFF 2/2 (ФИНАЛЬНАЯ ВЕРСИЯ) — crates/driver-mcp/src/lib.rs
//! Привязан к точным строкам, подтверждённым локальным агентом:
//!   - McpClientHandler struct: строки 64-77 (сейчас только `progress: ProgressDispatcher`)
//!   - from_session(): строка 146 (точка первичного discover_tools())
//!   - discover_tools(): строки 186-210 (пишет в tool_schemas: HashMap<String, Value>)
//!
//! Ключевое отличие от предыдущего наброска: НЕ нужен generic on_notification
//! fallback — rmcp 0.8.5 сам диспатчит notifications/tools/list_changed в
//! типизированный ClientHandler::on_tool_list_changed(). Реализуем именно
//! его, без ручного парсинга method-строки.
//!
//! Архитектурное решение по мотивам "Вариант Б" из предыдущего сообщения,
//! теперь конкретизированное под реальный код: канал mpsc, не Weak<Self>
//! цикл — потому что McpClientHandler создаётся ДО существования McpDriver
//! (тот же порядок инициализации serve() -> session, что уже используется
//! для progress: ProgressDispatcher), значит Weak<McpDriver> внутри
//! McpClientHandler физически невозможен в момент конструирования (Driver
//! ещё не существует, когда Handler создаётся). mpsc::Sender решает это:
//! Sender можно создать и передать Handler'у ДО того, как Receiver начнёт
//! слушаться в background-задаче, запущенной уже ПОСЛЕ создания McpDriver.

// ============================================================
// СТРОКИ 64-77 — McpClientHandler: добавлено поле list_changed_tx.
// ============================================================
//
// БЫЛО:
//   #[derive(Clone, Default)]
//   struct McpClientHandler {
//       progress: ProgressDispatcher,
//   }
//
// СТАЛО:
#[derive(Clone)]
struct McpClientHandler {
    progress: ProgressDispatcher,
    // НОВОЕ: канал сигналов "list_changed произошёл". capacity 1 +
    // try_send (не send().await) — если сигнал уже в очереди, второй не
    // нужен, background-задача всё равно переделает full re-discovery,
    // который покроет любые изменения, пришедшие между двумя нотификациями.
    // Не Default derive больше (mpsc::Sender не имеет Default) — конструктор
    // ниже.
    list_changed_tx: tokio::sync::mpsc::Sender<()>,
}

impl McpClientHandler {
    fn new(list_changed_tx: tokio::sync::mpsc::Sender<()>) -> Self {
        Self {
            progress: ProgressDispatcher::default(),
            list_changed_tx,
        }
    }
}

// ============================================================
// ClientHandler impl — добавлен on_tool_list_changed (типизированный
// метод SDK, подтверждено: диспатчится автоматически, без ручного
// разбора notification.method).
// ============================================================
impl ClientHandler for McpClientHandler {
    async fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.progress.handle_notification(params).await;
    }

    // НОВОЕ: точная сигнатура из rmcp 0.8.5, подтверждённая локальным
    // агентом (handler/client.rs) — impl Future, не async fn напрямую,
    // потому что это override default trait method с такой сигнатурой.
    fn on_tool_list_changed(
        &self,
        _context: NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {
            // try_send, не send().await — этот метод не должен блокироваться
            // и не должен паниковать, если канал полон (что означает
            // background-задача уже знает о необходимости re-discovery,
            // но ещё не успела её выполнить — второй сигнал избыточен).
            if self.list_changed_tx.try_send(()).is_err() {
                tracing::debug!("list_changed signal already pending, background watcher will still re-discover");
            }
        }
    }
}

// ============================================================
// from_session() — строка 146. Добавлен запуск background-задачи ПОСЛЕ
// первичного discover_tools(). Показан минимальный diff вокруг этой
// точки, остальное тело from_session() (session setup, allowed_tools,
// tool_schemas init) — без изменений.
// ============================================================
//
// БЫЛО (примерная реконструкция по описанию, реальные имена локальных
// переменных session/self могут отличаться — заменить по факту):
//   async fn from_session(session: RunningSession, allowed_tools: Vec<String>) -> Result<Self, McpDriverError> {
//       let driver = Self { session, allowed_tools, tool_schemas: Arc::new(RwLock::new(HashMap::new())), ... };
//       driver.discover_tools().await?;
//       Ok(driver)
//   }
//
// СТАЛО:
impl McpDriver {
    async fn from_session_with_registry_link(
        session: RunningSession,
        allowed_tools: Vec<String>,
        agent_id: adapter_model::AgentId,
        registry: std::sync::Weak<adapter_core::AgentRegistry>,
    ) -> Result<Arc<Self>, McpDriverError> {
        let (list_changed_tx, list_changed_rx) = tokio::sync::mpsc::channel::<()>(1);

        // handler создаётся здесь и передаётся в session.serve(handler) —
        // порядок ДО существования driver подтверждён вашим описанием
        // (progress: ProgressDispatcher уже так работает, list_changed_tx
        // добавляется тем же путём).
        let handler = McpClientHandler::new(list_changed_tx);

        let driver = Arc::new(Self {
            session, // предполагается, что session уже сконструирован с этим handler'ом выше по коду from_session — не меняем этот порядок
            allowed_tools,
            tool_schemas: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            // ... остальные поля McpDriver без изменений ...
        });

        driver.discover_tools().await?; // первичный discovery — БЕЗ ИЗМЕНЕНИЙ, тот же вызов, что уже есть на строке ~146

        // НОВОЕ: background-задача, реагирующая на list_changed. Держит
        // Arc<Self> (не Weak) — задача должна жить, пока жив driver;
        // Weak<AgentRegistry> — чтобы не мешать AgentRegistry освободиться
        // при shutdown.
        let driver_for_watcher = driver.clone();
        tokio::spawn(async move {
            let mut rx = list_changed_rx;
            while rx.recv().await.is_some() {
                match driver_for_watcher.discover_tools().await {
                    Ok(()) => {
                        let Some(registry) = registry.upgrade() else {
                            tracing::debug!("registry dropped, stopping list_changed watcher");
                            break;
                        };
                        let Some(registered_agent) = registry.get(&agent_id) else {
                            tracing::warn!(agent_id = %agent_id.0, "agent no longer in registry, stopping watcher");
                            break;
                        };
                        // tool_schemas: HashMap<String, Value> -> нужны только
                        // имена (ключи) для RegisteredAgent.skills — схемы
                        // (input_schema) сейчас не потребляются AgentRegistry,
                        // остаются доступны через driver.tool_schemas() для
                        // будущего использования (например, валидация
                        // аргументов до invoke), не теряются, просто не
                        // дублируются в skills.
                        let new_skill_names: Vec<String> = driver_for_watcher
                            .tool_schemas
                            .read()
                            .await
                            .keys()
                            .cloned()
                            .collect();
                        registered_agent.update_skills(new_skill_names).await;
                        tracing::info!(agent_id = %agent_id.0, "skills hot-updated after tools/list_changed");
                    }
                    Err(e) => {
                        tracing::warn!(agent_id = %agent_id.0, error = %e, "re-discovery after list_changed failed, keeping stale skill list");
                    }
                }
            }
        });

        Ok(driver)
    }
}

// ============================================================
// ВАЖНО: сигнатурное изменение публичного API connect_stdio()
// ============================================================
// connect_stdio() (или как называется публичный конструктор, вызываемый
// из adapterd/src/main.rs::build_driver()) должен теперь принимать
// agent_id и Weak<AgentRegistry> дополнительными параметрами, чтобы
// прокинуть их в from_session_with_registry_link(). Это единственная
// точка, где main.rs::build_driver() требует правки:
//
//   БЫЛО (main.rs, build_driver()):
//     AgentTransportConfig::Stdio если это MCP-вариант... (уточнить у
//     вас точный enum-вариант для MCP в AgentTransportConfig, не видел
//     его в config.rs — возможно там отдельный AgentTransportConfig::Mcp
//     ещё не добавлен, это отдельная зависимая правка)
//
//   СТАЛО: build_driver() должен иметь доступ к Weak<AgentRegistry> ДО
//   вызова MCP-варианта конструктора — но registry строится в build()
//   ПОСЛЕ builddriver() для каждого агента (текущий порядок в main.rs:
//   `for agent in config.agents { let driver = build_driver(agent).await?;
//   registry.register(...) }`) — то есть на момент build_driver() сам
//   Arc<AgentRegistry> уже существует (создан строкой раньше), просто
//   конкретный agent ещё не зарегистрирован в нём. Weak<AgentRegistry>
//   от уже существующего Arc<AgentRegistry> — передать можно, agent_id
//   тоже уже известен (это config.agents[i].id). Требуется правка сигнатуры
//   build_driver(agent: &AgentConfig, registry: &Arc<AgentRegistry>) —
//   один новый параметр.

#[derive(thiserror::Error, Debug)]
enum McpDriverError {
    #[error("tools/list failed: {0}")]
    ToolsList(String),
}
