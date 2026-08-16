//! `adapterd` — composition root и daemon lifecycle.
//!
//! Этот `lib.rs` ре-экспортирует `config` наружу, чтобы `adapterctl`
//! (installer/CLI) мог использовать те же типы конфигурации, что и сам
//! daemon, без дублирования парсера. Бинарь `main.rs` в этом же крейте
//! использует `mod config;` напрямую.

pub mod config;
