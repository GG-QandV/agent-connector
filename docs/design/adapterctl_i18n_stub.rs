//! crates/adapterctl/src/i18n.rs — English-only на этапе MVP, с заглушкой
//! под будущую полноценную i18n-инфраструктуру.
//!
//! Решение (пересмотрено после ревью): полноценный i18n с параллельным
//! переводом на 2 языка был лишним шагом для MVP — либо делать масштабируемо
//! на 25-40 языков сразу (Fluent/ICU MessageFormat + внешние .ftl файлы,
//! процесс перевода, contributor workflow), либо не делать вовсе. Промежуточное
//! состояние "en+ru захардкожены в match" не масштабируется и создаёт
//! технический долг: 25-й язык всё равно требует полного переписывания
//! механизма (Vec<&str> match -> lookup по файлам/каталогу переводов).
//!
//! Текущее решение: единственный язык — English. Слой i18n:: существует
//! как ЕДИНСТВЕННАЯ точка входа для текста, чтобы install_flow.rs/
//! managed_docker.rs и остальная бизнес-логика НЕ содержали строк напрямую —
//! это остаётся архитектурно верным независимо от того, 1 язык или 40.
//! Когда i18n реально понадобится, меняется только internals этого модуля
//! (Msg::render() -> lookup в Fluent bundle), вызывающий код в install_flow.rs
//! не меняется вообще — сигнатура `Msg::render(self) -> Cow<'static, str>`
//! остаётся стабильной.

use std::borrow::Cow;

/// Все CLI-сообщения — exhaustive enum ключей, English-only текст.
/// Тот же принцип, что и в двухъязычной версии: компилятор гарантирует,
/// что каждый ключ имеет текст (exhaustive match), просто на один язык.
#[derive(Clone, Copy)]
pub enum Msg {
    StorageSelectPrompt,
    StorageOptionSqlite,
    StorageOptionExistingPostgres,
    StorageOptionManagedDocker,
    StorageOptionExternalManaged,
    StorageChoicePromptLine,
    StorageChoiceInvalid,
    NoTtyError,
    DockerConfirmRequired,
    DockerNotReachable,
    DockerWrongContainerMode,
    PostgresValidationFailed,
    InstallSuccessHeader,
    InstallSummaryPrefix,
    InstallSummaryBinary,
    InstallSummaryConfig,
    InstallSummaryData,
    InstallSummaryPostgresLocalhost,
    InstallSummaryPostgresNetworkOnly,
    InstallSummaryPgVolumeNote,
    InstallSummaryBackupCommand,
    InstallSummaryUninstallCommand,
    UninstallDataPreservedNote,
    UninstallDataPurgedNote,
    RequiresAdminWindows,
    RequiresRootUnix,
}

impl Msg {
    /// Возвращает `Cow<'static, str>`, не `&'static str` — сознательный
    /// выбор сигнатуры на будущее: когда появится реальный i18n backend
    /// (Fluent bundle lookup), результат часто будет owned String
    /// (интерполяция плейсхолдеров, множественные формы), а не статичный
    /// слайс. Меняя внутреннюю реализацию потом, сигнатуру менять не придётся,
    /// вызывающий код (install_flow.rs) её не увидит вообще.
    pub fn render(self) -> Cow<'static, str> {
        Cow::Borrowed(match self {
            Msg::StorageSelectPrompt => "Select a storage backend for agent-connector:",
            Msg::StorageOptionSqlite =>
                "  1) SQLite            — single file, no external dependencies, good for single-node/dev",
            Msg::StorageOptionExistingPostgres =>
                "  2) Existing Postgres — you already have a Postgres instance to connect to",
            Msg::StorageOptionManagedDocker =>
                "  3) Managed Docker Postgres — installer runs an isolated Postgres container for this instance only",
            Msg::StorageOptionExternalManaged =>
                "  4) External managed Postgres — RDS, Neon, Supabase, Cloud SQL, etc. (same as #2, different wording)",
            Msg::StorageChoicePromptLine => "Choice [1-4]: ",
            Msg::StorageChoiceInvalid => "invalid choice, expected 1-4",
            Msg::NoTtyError =>
                "no TTY available for interactive prompt — pass --storage explicitly in non-interactive environments",
            Msg::DockerConfirmRequired =>
                "managed-docker-postgres requires --confirm-docker (installer will not install, start, or pull images without explicit confirmation)",
            Msg::DockerNotReachable => "Docker daemon is not reachable — is Docker Desktop running?",
            Msg::DockerWrongContainerMode =>
                "Docker is in Windows containers mode, but a Linux-based Postgres image is required. Switch to Linux containers.",
            Msg::PostgresValidationFailed => "Postgres connection validation failed",
            Msg::InstallSuccessHeader => "agent-connector installed successfully.",
            Msg::InstallSummaryPrefix => "  prefix:    ",
            Msg::InstallSummaryBinary => "  binary:    ",
            Msg::InstallSummaryConfig => "  config:    ",
            Msg::InstallSummaryData => "  data:      ",
            Msg::InstallSummaryPostgresLocalhost => "  postgres:  127.0.0.1 (localhost only)",
            Msg::InstallSummaryPostgresNetworkOnly => "  postgres:  internal Docker network only",
            Msg::InstallSummaryPgVolumeNote => "(NOT removed on uninstall unless --purge-data)",
            Msg::InstallSummaryBackupCommand => "  backup:    ",
            Msg::InstallSummaryUninstallCommand => "  uninstall: ",
            Msg::UninstallDataPreservedNote => "Data preserved: re-run with --purge-data to delete permanently.",
            Msg::UninstallDataPurgedNote =>
                "--purge-data set: removing data directory and managed Docker volume/container.",
            Msg::RequiresAdminWindows => "adapterctl must be run from an elevated (Administrator) PowerShell/cmd prompt",
            Msg::RequiresRootUnix => "adapterctl must be run as root (sudo adapterctl ...)",
        })
    }
}

// ============================================================
// FUTURE: полноценная i18n-инфраструктура (не реализовывать сейчас,
// зафиксировано здесь как явная точка расширения, чтобы будущий
// разработчик не изобретал архитектуру заново).
// ============================================================
//
// Когда появится реальная потребность в 25-40 языках:
//
// 1. Заменить строковый match выше на lookup в Fluent (fluent-rs crate) —
//    ICU MessageFormat поддерживает плюрализацию, интерполяцию, gender
//    agreement, что голый match никогда не покрывал бы для 40 языков.
//
// 2. Файлы переводов — `locales/{lang}/adapterctl.ftl`, ОДИН каталог, не
//    Rust-код — позволяет внешним переводчикам (Crowdin/Weblate workflow)
//    контрибьютить без пересборки бинаря на этапе перевода (только на
//    релиз, embed через build.rs/include_dir!).
//
// 3. Определение языка (тот механизм из предыдущей ревизии остаётся
//    валидным на будущее, просто не реализуется сейчас):
//      --lang флаг > ADAPTERCTL_LANG env > LC_ALL/LC_MESSAGES/LANG > English fallback
//
// 4. Msg::render(self) -> Cow<'static, str> сигнатура УЖЕ учитывает это —
//    become Msg::render(self, lang: Lang, args: &FluentArgs) -> Cow<'_, str>,
//    вызывающий код в install_flow.rs просто получит дополнительный
//    параметр `lang`, сам паттерн вызова (`msg.render()` в println!)
//    не меняется структурно.
//
// 5. Триггер для реализации: явный запрос от пользователей на конкретный
//    язык, не "на будущее" — YAGNI применительно к i18n особенно verno,
//    потому что стоимость поддержки N языков растёт с каждым добавленным
//    CLI-сообщением, а не только на старте.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_renders_non_empty() {
        // Простая защита от опечатки типа `Msg::Foo => ""` — не даёт
        // компилятору поймать, но тест ловит сразу.
        let all = [
            Msg::StorageSelectPrompt, Msg::StorageOptionSqlite, Msg::StorageOptionExistingPostgres,
            Msg::StorageOptionManagedDocker, Msg::StorageOptionExternalManaged, Msg::StorageChoicePromptLine,
            Msg::StorageChoiceInvalid, Msg::NoTtyError, Msg::DockerConfirmRequired, Msg::DockerNotReachable,
            Msg::DockerWrongContainerMode, Msg::PostgresValidationFailed, Msg::InstallSuccessHeader,
            Msg::InstallSummaryPrefix, Msg::InstallSummaryBinary, Msg::InstallSummaryConfig, Msg::InstallSummaryData,
            Msg::InstallSummaryPostgresLocalhost, Msg::InstallSummaryPostgresNetworkOnly, Msg::InstallSummaryPgVolumeNote,
            Msg::InstallSummaryBackupCommand, Msg::InstallSummaryUninstallCommand, Msg::UninstallDataPreservedNote,
            Msg::UninstallDataPurgedNote, Msg::RequiresAdminWindows, Msg::RequiresRootUnix,
        ];
        for msg in all {
            assert!(!msg.render().is_empty(), "message must not render as empty string");
        }
    }
}
