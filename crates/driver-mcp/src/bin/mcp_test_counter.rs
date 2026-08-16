//! Минимальный MCP test server — порт `examples/servers/src/counter_stdio.rs`
//! из `modelcontextprotocol/rust-sdk`, адаптированный под rmcp 0.8.5 API.
//!
//! Поддерживает два транспорта:
//! - stdio (по умолчанию): запускается integration test'ами через
//!   `CARGO_BIN_EXE_mcp_test_counter`;
//! - streamable HTTP: если задана env `MCP_TEST_COUNTER_HTTP=127.0.0.1:PORT`,
//!   сервер слушает HTTP на этом адресе (endpoint `POST /mcp`).
//!
//! Tools: `increment`, `get_value` (счётчик) и `long_task` (sleep 10s — для
//! теста cancel).

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    schemars, tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    ServerHandler, ServiceExt,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Counter {
    counter: Arc<Mutex<i32>>,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct EchoRequest {
    pub message: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DelayedRequest {
    pub delay_ms: u64,
}

#[tool_router(router = tool_router)]
impl Counter {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(Mutex::new(0)),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Increment the counter by 1")]
    async fn increment(&self) -> Result<String, rmcp::ErrorData> {
        let mut counter = self.counter.lock().await;
        *counter += 1;
        Ok(counter.to_string())
    }

    #[tool(description = "Get the current counter value")]
    async fn get_value(&self) -> Result<String, rmcp::ErrorData> {
        let counter = self.counter.lock().await;
        Ok(counter.to_string())
    }

    #[tool(description = "Echo the message back")]
    async fn echo(
        &self,
        Parameters(request): Parameters<EchoRequest>,
    ) -> Result<String, rmcp::ErrorData> {
        Ok(request.message)
    }

    #[tool(description = "Sleep for delay_ms then return a fixed string")]
    async fn delayed(
        &self,
        Parameters(request): Parameters<DelayedRequest>,
    ) -> Result<String, rmcp::ErrorData> {
        tokio::time::sleep(Duration::from_millis(request.delay_ms)).await;
        Ok(format!("slept {}ms", request.delay_ms))
    }

    #[tool(description = "Long running task example")]
    async fn long_task(&self) -> Result<String, rmcp::ErrorData> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok("Long task completed".to_string())
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Counter {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(addr) = std::env::var("MCP_TEST_COUNTER_HTTP") {
        return run_http(addr).await;
    }
    let service = Counter::new()
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| format!("serving error: {e:?}"))?;
    service.waiting().await?;
    Ok(())
}

async fn run_http(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let service: StreamableHttpService<Counter, LocalSessionManager> = StreamableHttpService::new(
        || Ok(Counter::new()),
        Default::default(),
        StreamableHttpServerConfig {
            stateful_mode: true,
            sse_keep_alive: None,
        },
    );
    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let actual = listener.local_addr()?;
    eprintln!("MCP counter HTTP server listening on http://{actual}/mcp");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
