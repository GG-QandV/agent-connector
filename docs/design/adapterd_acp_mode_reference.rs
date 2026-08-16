//! REFERENCE-реализация ACP stdio-режима внутри `adapterd`.
//!
//! ВАЖНО: это не drop-in patch — я не видел полный текст вашего реального
//! main.rs/config.rs, поэтому имена полей Config/AdapterdConfig ниже —
//! наилучшее предположение на основе увиденных фрагментов и вашего описания
//! (`servers.a2a_http`, `servers.acp_stdio`, `shutdown_grace_seconds`).
//! Адаптируйте имена под реальную структуру перед вставкой.
//!
//! Зависит от:
//! - adapter_core::AdapterCore (видел полностью)
//! - protocol_acp_runtime::{AcpRuntime, AcpRuntimeConfig, StdinOut} (видел полностью)
//! - protocol_a2a_server::health::{HealthState, health_router} (видел полностью)
//! - tokio_util::sync::CancellationToken

use adapter_core::AdapterCore;
use protocol_acp_runtime::{AcpRuntime, AcpRuntimeConfig, StdinOut};
use std::sync::{atomic::AtomicBool, Arc};
use tokio::io::{stdin, stdout, BufReader};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Конфиг ACP stdio-транспорта. Подставьте под реальный AdapterdConfig —
/// вероятно это уже существующая секция `servers.acp_stdio` в config.rs.
#[derive(Clone, Debug)]
pub struct AcpStdioServerConfig {
    pub enabled: bool,
    pub max_line_bytes: usize,
    pub shutdown_grace_seconds: u64,
    pub agent_name: String,
    pub agent_version: String,
}

impl Default for AcpStdioServerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_line_bytes: 1024 * 1024,
            shutdown_grace_seconds: 5,
            agent_name: "agent-connector".into(),
            agent_version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

/// Аналогичный заглушечный тип для A2A HTTP — предполагается, что он уже
/// существует в вашем config.rs под другим именем; здесь только для того,
/// чтобы показать полную картину проверки "хотя бы один транспорт включён".
#[derive(Clone, Debug)]
pub struct A2aHttpServerConfig {
    pub enabled: bool,
}

/// Точка входа transport-mode orchestration. Вызывается из adapterd::main()
/// после того, как AdapterCore уже собран (store, registry, policy).
///
/// Возвращает JoinHandle-и обоих транспортов (те, что enabled) и общий
/// CancellationToken для координации shutdown.
pub struct RunningTransports {
    pub shutdown: CancellationToken,
    pub draining: Arc<AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl RunningTransports {
    /// Дождаться штатного shutdown-сигнала (SIGINT/SIGTERM) и корректно
    /// остановить все запущенные транспорты в пределах grace period.
    pub async fn wait_for_shutdown_and_stop(self, grace_seconds: u64) {
        let _ = signal::ctrl_c().await;
        info!("shutdown signal received, draining");
        self.draining.store(true, std::sync::atomic::Ordering::SeqCst);
        self.shutdown.cancel();

        let grace = std::time::Duration::from_secs(grace_seconds);
        let all_done = futures_util::future::join_all(self.handles);
        match tokio::time::timeout(grace, all_done).await {
            Ok(_) => info!("all transports stopped cleanly within grace period"),
            Err(_) => warn!(
                grace_seconds,
                "shutdown grace period exceeded; some transports may not have stopped cleanly"
            ),
        }
    }
}

/// Запустить включённые транспорты. Паникует при старте, если ни один
/// транспорт не включён — процесс без единого способа принимать задачи
/// не имеет смысла запускать.
pub fn spawn_transports(
    core: Arc<AdapterCore>,
    a2a_config: A2aHttpServerConfig,
    acp_config: AcpStdioServerConfig,
    // health-state строится тем же способом, что и раньше (task_store, registry,
    // draining) — здесь передаётся уже готовый Arc, чтобы не пересобирать.
    health_state_factory: impl FnOnce(Arc<AtomicBool>) -> protocol_a2a_server::health::HealthState,
    // фабрика A2A axum::Router без health — health монтируется здесь отдельно
    // и одинаково независимо от того, собран ли остальной A2A router.
    a2a_router_factory: Option<impl FnOnce() -> axum::Router>,
    bind_addr: Option<std::net::SocketAddr>,
) -> RunningTransports {
    if !a2a_config.enabled && !acp_config.enabled {
        panic!(
            "adapterd misconfigured: neither servers.a2a_http.enabled nor \
             servers.acp_stdio.enabled is true — process has no way to receive tasks"
        );
    }
    if a2a_config.enabled && acp_config.enabled {
        warn!(
            "both A2A HTTP and ACP stdio transports are enabled in the same \
             process; this is unusual outside dev/test scenarios — ACP stdio \
             expects to own this process's stdin/stdout exclusively"
        );
    }

    let shutdown = CancellationToken::new();
    let draining = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    if a2a_config.enabled {
        let Some(router_factory) = a2a_router_factory else {
            panic!("servers.a2a_http.enabled=true but no router factory provided");
        };
        let Some(addr) = bind_addr else {
            panic!("servers.a2a_http.enabled=true but no bind address configured");
        };
        let health_state = health_state_factory(draining.clone());
        let router = router_factory().merge(protocol_a2a_server::health::health_router(health_state));
        let shutdown_a2a = shutdown.clone();

        let handle = tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!(error = %e, %addr, "failed to bind A2A HTTP listener");
                    return;
                }
            };
            info!(%addr, "A2A HTTP transport listening");
            let serve = axum::serve(listener, router.into_make_service());
            let graceful = serve.with_graceful_shutdown(async move {
                shutdown_a2a.cancelled().await;
                info!("A2A HTTP transport draining");
            });
            if let Err(e) = graceful.await {
                error!(error = %e, "A2A HTTP transport exited with error");
            }
        });
        handles.push(handle);
    }

    if acp_config.enabled {
        let runtime_config = AcpRuntimeConfig {
            max_line_bytes: acp_config.max_line_bytes,
            shutdown_grace: std::time::Duration::from_secs(acp_config.shutdown_grace_seconds),
            agent_id: "adapter".into(),
            agent_name: acp_config.agent_name.clone(),
            agent_version: acp_config.agent_version.clone(),
            capabilities: serde_json::json!({
                "filesystem": false,
                "terminal": false,
                "streaming": true,
                "cancellation": true,
                "sessionResume": true
            }),
        };
        let io = StdinOut {
            reader: BufReader::new(stdin()),
            writer: stdout(),
        };
        let mut runtime = AcpRuntime::new(core.clone(), runtime_config, "acp-stdio", io);
        let shutdown_acp = shutdown.clone();

        let handle = tokio::spawn(async move {
            info!("ACP stdio transport listening on process stdin/stdout");
            runtime.run_with_shutdown(shutdown_acp).await;
            info!("ACP stdio transport stopped (EOF or shutdown)");
        });
        handles.push(handle);
    }

    RunningTransports {
        shutdown,
        draining,
        handles,
    }
}

// --- Пример использования внутри adapterd::main() ---
//
// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     tracing_subscriber::fmt::init();
//     let config = load_config()?;                       // существующий код
//     let core = build_adapter_core(&config).await?;      // существующий код
//
//     let running = spawn_transports(
//         core,
//         config.servers.a2a_http.clone(),
//         config.servers.acp_stdio.clone(),
//         |draining| {
//             protocol_a2a_server::health::HealthState::new(
//                 task_store.clone(),
//                 registry.clone(),
//                 draining,
//             )
//         },
//         Some(|| build_a2a_router(core.clone())),          // существующий код
//         Some(config.servers.a2a_http.bind.parse()?),
//     );
//
//     running.wait_for_shutdown_and_stop(config.runtime.shutdown_grace_seconds).await;
//     Ok(())
// }
