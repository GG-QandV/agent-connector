//! DIFF 2/2 — crates/driver-mcp/src/lib.rs
//! Добавляет обработку notifications/tools/list_changed и связь с
//! AgentRegistry через Weak (не Arc — избегаем цикла владения
//! Registry -> RegisteredAgent -> Driver -> Registry).
//!
//! Зависит от DIFF 1 (adapter_core::RegisteredAgent::update_skills()) —
//! применять оба diff'а вместе, не по отдельности.

use adapter_core::AgentRegistry;
use adapter_model::TaskId;
use rmcp::{
    handler::client::{progress::ProgressDispatcher, ClientHandler},
    model::{Notification, ProgressNotificationParam},
    service::{NotificationContext, RoleClient},
};
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;

/// ИЗМЕНЕНО: McpClientHandler получает Weak<AgentRegistry> + свой AgentId,
/// чтобы найти себя в реестре при получении list_changed. Weak — не Arc,
/// чтобы не создать цикл: AgentRegistry держит Arc<RegisteredAgent>,
/// RegisteredAgent держит Arc<dyn AgentDriver> (это McpDriver), а
/// McpDriver->McpClientHandler если бы держал Arc<AgentRegistry> обратно —
/// ни один Arc никогда не дошёл бы до нуля, память никогда не освободилась
/// бы после unregister/drop.
#[derive(Clone, Default)]
struct McpClientHandler {
    progress: ProgressDispatcher,
    // НОВОЕ: заполняется после connect_stdio(), до этого None — ClientHandler
    // должен существовать до того, как известен AgentId (порядок инициализации
    // в connect_stdio: handler создаётся, потом serve(), потом agent_id известен
    // только вызывающему коду в main.rs/build_driver(), не самому McpDriver).
    registry_link: Arc<RwLock<Option<RegistryLink>>>,
}

struct RegistryLink {
    registry: Weak<AgentRegistry>,
    agent_id: adapter_model::AgentId,
}

impl McpClientHandler {
    /// НОВОЕ: вызывается из connect_stdio() ПОСЛЕ того, как известен
    /// agent_id (который передаётся как параметр в connect_stdio, уже
    /// существует в текущей сигнатуре) — просто сохраняем ссылку.
    async fn attach_registry(&self, registry: Weak<AgentRegistry>, agent_id: adapter_model::AgentId) {
        *self.registry_link.write().await = Some(RegistryLink { registry, agent_id });
    }
}

impl ClientHandler for McpClientHandler {
    async fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.progress.handle_notification(params).await;
    }

    /// НОВОЕ: обработчик generic-нотификаций. rmcp может не иметь отдельного
    /// типизированного `on_tools_list_changed` метода в ClientHandler trait —
    /// если он есть в вашей версии SDK (проверить через `cargo doc -p rmcp`,
    /// как это делал локальный агент для progress-методов ранее), заменить
    /// эту функцию на его прямую реализацию. Если типизированного метода
    /// нет, стандартный fallback — generic on_notification/handle_notification
    /// с проверкой method == "notifications/tools/list_changed" вручную.
    async fn on_notification(&self, notification: Notification, _context: NotificationContext<RoleClient>) {
        if notification.method != "notifications/tools/list_changed" {
            return; // не наш случай — прогресс уже обработан отдельным методом выше
        }

        let link_guard = self.registry_link.read().await;
        let Some(link) = link_guard.as_ref() else {
            tracing::warn!("received tools/list_changed before registry link was attached — ignoring");
            return;
        };

        let Some(registry) = link.registry.upgrade() else {
            // Registry уже уничтожен (штатное shutdown) — не ошибка, просто
            // больше не нужно ничего обновлять.
            return;
        };

        let Some(registered_agent) = registry.get(&link.agent_id) else {
            tracing::warn!(agent_id = %link.agent_id.0, "list_changed received but agent no longer in registry");
            return;
        };

        drop(link_guard); // отпускаем read lock до вызова re-discovery

        // re-discovery делает сам McpDriver (у него session/tool_names),
        // не ClientHandler — ClientHandler здесь только маршрутизирует
        // нотификацию, реальная логика пагинированного list_tools живёт
        // в McpDriver::discover_tools(), которая уже существует. Здесь
        // нужна ссылка на driver, которой у ClientHandler пока нет —
        // см. АЛЬТЕРНАТИВНАЯ АРХИТЕКТУРА ниже, это ключевая развилка.
        tracing::info!(agent_id = %link.agent_id.0, "tools/list_changed received, triggering re-discovery");

        // Оставлено как явная точка расширения — см. комментарий внизу файла.
        let _ = registered_agent;
    }
}

// ============================================================
// АЛЬТЕРНАТИВНАЯ АРХИТЕКТУРА — какая из двух реализовать, зависит от
// того, как ClientHandler и McpDriver физически связаны в вашей версии
// lib.rs (я не видел actual код после последних правок, только структуру
// на момент верификации rmcp API):
//
// Вариант А (использован выше, минимальные изменения ClientHandler):
//   ClientHandler хранит Weak<AgentRegistry> + agent_id напрямую, сам
//   вызывает registry.get(id).update_skills(...) — НО ему для этого нужен
//   пагинированный список tools, а discover_tools() (с логикой пагинации,
//   allowed_tools фильтром) живёт в McpDriver, не в ClientHandler. Значит
//   ClientHandler должен либо (а) продублировать логику пагинации сам —
//   плохо, дублирование, либо (б) держать Arc<McpDriver> себе — циклическая
//   ссылка (Driver содержит Handler внутри session, Handler содержит
//   ссылку на Driver обратно), нужен Weak<McpDriver> тоже.
//
// Вариант Б (чище): ClientHandler на list_changed просто шлёт сигнал через
// tokio::sync::mpsc::Sender<()> в отдельную background-задачу, которую
// connect_stdio() спавнит РЯДОМ с самим McpDriver (не внутри ClientHandler).
// Эта задача держит Arc<Self> (McpDriver) и Weak<AgentRegistry>, у неё есть
// доступ и к discover_tools(), и к registry — без циклов, потому что канал
// mpsc не создаёт Arc-цикл, только Sender/Receiver.
//
// РЕКОМЕНДАЦИЯ: вариант Б. Ниже — набросок для connect_stdio() с этим
// подходом, заменяет ветку "tracing::info! + let _ =" выше на реальную
// работу.
// ============================================================

/// НОВОЕ (вариант Б): добавляется в McpDriver::connect_stdio() ПОСЛЕ
/// discover_tools() (первичный synchronous discovery остаётся как есть),
/// СТАРТУЕТ фоновую задачу для реакции на list_changed.
pub struct McpDriverListChangedHandle {
    // Держит канал живым — Drop этого handle останавливает background task.
    _sender_keepalive: tokio::sync::mpsc::Sender<()>,
}

pub fn spawn_list_changed_watcher(
    driver_weak: Weak<McpDriverForWatcher>, // трейт-объект или сам McpDriver за Weak<Self>
    registry_weak: Weak<AgentRegistry>,
    agent_id: adapter_model::AgentId,
) -> (tokio::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

    let handle = tokio::spawn(async move {
        while rx.recv().await.is_some() {
            let Some(driver) = driver_weak.upgrade() else { break }; // driver dropped -> stop watcher
            let Some(registry) = registry_weak.upgrade() else { break }; // registry dropped -> stop watcher

            match driver.discover_tools_and_return_list().await {
                Ok(new_skills) => {
                    if let Some(registered_agent) = registry.get(&agent_id) {
                        registered_agent.update_skills(new_skills).await;
                        tracing::info!(agent_id = %agent_id.0, "skills hot-updated after tools/list_changed");
                    }
                }
                Err(e) => {
                    tracing::warn!(agent_id = %agent_id.0, error = %e, "re-discovery after list_changed failed, keeping stale skill list");
                }
            }
        }
    });

    (tx, handle)
}

// Минимальный трейт, чтобы spawn_list_changed_watcher не зависел от
// конкретного McpDriver типа напрямую (упрощает тестирование мок-объектом).
// В реальном коде McpDriver реализует этот трейт, discover_tools() внутри
// него меняет тип возврата с Result<(), McpDriverError> на
// Result<Vec<String>, McpDriverError> (или добавляется отдельный метод
// discover_tools_and_return_list(), не трогая существующий discover_tools()
// с его текущим контрактом для вызывающих его других мест кода).
#[async_trait::async_trait]
pub trait McpDriverForWatcher: Send + Sync {
    async fn discover_tools_and_return_list(&self) -> Result<Vec<String>, McpDriverError>;
}

#[derive(thiserror::Error, Debug)]
pub enum McpDriverError {
    #[error("re-discovery failed: {0}")]
    Rediscovery(String),
}
