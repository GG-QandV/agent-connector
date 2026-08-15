//! `protocol-acp-runtime` — ACP stdio JSON-RPC 2.0 runtime.
//!
//! ACP (agentclientprotocol.com, protocol v1) — JSON-RPC 2.0 over stdio,
//! newline-delimited, без embedded newline в одной строке; stdout зарезервирован
//! только под protocol messages.
//!
//! Подтверждённого готового Rust SDK ACP в workspace нет (a2a-rs — это A2A,
//! другой протокол). Реализуем типизированный JSON-RPC 2.0 loop самостоятельно
//! по официальной спецификации ACP, используя `protocol-acp-mapper` для
//! перевода в команды `AdapterCore`. Ошибки: parse error / invalid request
//! (JSON-RPC envelope), метод-специфичные — через mapper.
//!
//! Свойства (Блок 2):
//! - построчный read loop; каждая строка — один JSON-RPC message;
//! - malformed JSON → parse error как protocol message, loop продолжает;
//! - invalid envelope → invalid-request error (с тем же id, если извлечён);
//! - notification (без id) → никогда не пишем в stdout;
//! - max line size из конфига; превышение → explicit error до mapper/core;
//! - stdout — только валидные JSON-RPC строки + newline, flush после записи;
//! - все логи/диагностика — в stderr через tracing;
//! - draining на cancel-token; EOF → чистый выход без panic.

pub mod codec;
pub mod runtime;

pub use codec::{JsonRpcError, JsonRpcId, JsonRpcRequest, JsonRpcResponse};
pub use runtime::{AcpRuntime, AcpRuntimeConfig, StdinOut};
