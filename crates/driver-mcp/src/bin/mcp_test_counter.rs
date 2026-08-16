//! Минимальный MCP stdio test server — порт `examples/servers/src/counter_stdio.rs`
//! из `modelcontextprotocol/rust-sdk`, адаптированный под rmcp 0.8.5 API
//! (tool_handler/tool_router без `server_handler`). Три tools: `increment`,
//! `get_value` и `long_task` (sleep 10s — для теста cancel).
//!
//! Запускается integration test'ами `driver-mcp` через `CARGO_BIN_EXE_mcp_test_counter`.

use rmcp::{
    handler::server::router::tool::ToolRouter, tool, tool_handler, tool_router, ServerHandler,
    ServiceExt,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Counter {
    counter: Arc<Mutex<i32>>,
    tool_router: ToolRouter<Self>,
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

    #[tool(description = "Long running task example")]
    async fn long_task(&self) -> Result<String, rmcp::ErrorData> {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
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
    let service = Counter::new()
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| format!("serving error: {e:?}"))?;

    service.waiting().await?;
    Ok(())
}
