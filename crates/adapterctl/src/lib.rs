//! `adapterctl` — installer/CLI для agent-connector.
//!
//! Бинарь `main.rs` — точка входа CLI. Этот `lib.rs` существует, чтобы
//! integration-тесты (tests/) могли импортировать модули (`managed_docker`,
//! `install_flow`, ...) без запуска CLI.

pub mod cancel;
pub mod config_template;
pub mod install_flow;
pub mod managed_docker;
pub mod platform;
pub mod postgres_lifecycle;
