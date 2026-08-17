//! crates/driver-a2a-client/src/error.rs
//!
//! Единый тип ошибок driver-a2a-client. Покрывает транспорт, протокол JSON-RPC
//! и специфичные для A2A-домена ситуации (context lost, method mismatch).

// ============================================================================
// ПРАВКА (из ACP-A2A_gateway docs/правки 2/в2/error_fix.rs):
//
// Исправляет дефект, найденный по факту из кода шлюза (transport_http.rs):
//
// - dispatch_a2a_method на НЕИЗВЕСТНЫЙ МЕТОД (нет такой ветки match) →
//   anyhow::bail!("method_not_found: {other}") → в rpc_handler это обычный
//   Err(e), не ContextLost → generic ветка → status=200, code=-32000
//   (transport_http.rs:236, комментарий "Диапазон -32000..-32099 отведён
//   JSON-RPC под ошибки приложения").
// - -32601 шлюз отдаёт ТОЛЬКО из AdapterError::rpc_code() для варианта
//   UnknownAgent — то есть когда сам agent_id не найден в registry
//   (transport_http.rs:105), а не когда метод не распознан.
//
// Прежняя реализация from_jsonrpc_error матчила только code == -32601 как
// признак "неверный wire_format" — то есть SDK-клиент с опечаткой в методе
// (например "sendMessage" lowercase, которого нет ни в одной ветке
// dispatch_a2a_method) получит -32000 с текстом "method_not_found: sendMessage"
// и провалится в общий RemoteError без подсказки про wire_format, хотя
// причина ошибки ровно та же — несовпадение имени метода с ожидаемым
// диалектом сервера.
//
// Исправление: подсказка про wire_format должна срабатывать по ДВУМ
// независимым условиям:
//   1. code == -32601 (агент не найден — тоже стоит упомянуть wire_format
//      в диагностике на всякий случай, но это не является железным
//      признаком неверного формата; оставляем как раньше, отдельным
//      вариантом ошибки, НЕ переименовываем в MethodNotFound).
//   2. code == -32000 И message содержит подстроку "method_not_found:" —
//      это и есть настоящий признак "метод не распознан диспетчером",
//      который куда чаще указывает на wire_format mismatch, чем на
//      реальную опечатку в имени агента.
//
// Оба случая сводятся в один вариант ошибки MethodNotFound, чтобы клиентский
// код (например driver-a2a-client::lib.rs) мог единообразно реагировать на
// "неправильный формат", независимо от того, какой конкретно код вернул
// сервер в конкретной версии/сборке.
// ============================================================================

use std::fmt;

#[derive(Debug)]
pub enum A2aClientError {
    /// Ошибка транспорта (сеть, таймаут, невалидный HTTP-ответ).
    Http(String),

    /// Тело ответа не соответствует ожидаемой JSON-RPC структуре
    /// (нет `result`, нет `task`, отсутствуют обязательные поля).
    ProtocolError(String),

    /// Сервер не смог сопоставить запрошенный метод со своим диалектом.
    /// Срабатывает на двух разных кодах шлюза (см. from_jsonrpc_error):
    /// -32601 (агент не найден — само по себе не про формат, но включено
    /// в эту категорию из осторожности) и -32000 с текстом
    /// "method_not_found:" (настоящий признак несовпадения метода).
    MethodNotFound {
        method: String,
        wire_format: &'static str,
        /// Какой именно код прислал сервер — важно для диагностики: -32601
        /// и -32000/method_not_found различаются по вероятной причине,
        /// и текст ошибки должен честно называть источник.
        server_code: i64,
        server_message: String,
    },

    /// Сервер вернул `-32010` (потеря контекста, например после перезапуска
    /// агента за шлюзом). Retryable: клиент должен начать новый context_id.
    ContextLost { server_message: String },

    /// Любая прочая ошибка уровня приложения (`-32000` и остальные коды).
    RemoteError { code: i64, message: String },
}

impl fmt::Display for A2aClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            A2aClientError::Http(msg) => write!(f, "A2A transport error: {msg}"),
            A2aClientError::ProtocolError(msg) => write!(f, "A2A protocol error: {msg}"),
            A2aClientError::MethodNotFound {
                method,
                wire_format,
                server_code,
                server_message,
            } => write!(
                f,
                "Method '{method}' not recognized by server (wire_format={wire_format}, \
                 server_code={server_code}). Hint: check that the endpoint's wire_format \
                 matches the server's expected JSON-RPC dialect (sdk ↔ SendMessage/GetTask/ \
                 CancelTask, spec ↔ message/send/tasks/get/tasks/cancel). Note: servers may \
                 report this as -32601 (unknown agent path) or as a generic application error \
                 (e.g. -32000) with 'method_not_found:' in the message — both are treated as \
                 the same class of error here. Server said: {server_message}"
            ),
            A2aClientError::ContextLost { server_message } => write!(
                f,
                "A2A context lost (-32010), likely due to agent restart behind the \
                 gateway. Retry with a fresh context_id (do not reuse task_id). \
                 Server said: {server_message}"
            ),
            A2aClientError::RemoteError { code, message } => {
                write!(f, "A2A remote error [{code}]: {message}")
            }
        }
    }
}

impl std::error::Error for A2aClientError {}

/// Подстрока, по которой распознаётся "метод не найден диспетчером" внутри
/// generic application-ошибки (-32000 и подобные). Взято дословно из
/// формата, который формирует dispatch_a2a_method на шлюзе:
/// `anyhow::bail!("method_not_found: {other}")`.
const METHOD_NOT_FOUND_MARKER: &str = "method_not_found:";

/// Строит ошибку из тела JSON-RPC `error` объекта, зная метод и wire_format
/// текущего запроса — чтобы MethodNotFound сразу содержал подсказку.
pub fn from_jsonrpc_error(
    code: i64,
    message: &str,
    method: &str,
    wire_format: &'static str,
) -> A2aClientError {
    let looks_like_unknown_method = code == -32601 || message.contains(METHOD_NOT_FOUND_MARKER);

    if looks_like_unknown_method {
        return A2aClientError::MethodNotFound {
            method: method.to_string(),
            wire_format,
            server_code: code,
            server_message: message.to_string(),
        };
    }

    match code {
        -32010 => A2aClientError::ContextLost {
            server_message: message.to_string(),
        },
        other => A2aClientError::RemoteError {
            code: other,
            message: message.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_32601_is_still_method_not_found() {
        let err = from_jsonrpc_error(-32601, "unknown agent_id: hermes", "SendMessage", "sdk");
        assert!(matches!(
            err,
            A2aClientError::MethodNotFound {
                server_code: -32601,
                ..
            }
        ));
    }

    /// Регрессия, найденная по факту из реального кода шлюза: неизвестный
    /// метод на общем диспетчере даёт -32000, НЕ -32601. Прежняя версия
    /// from_jsonrpc_error пропускала этот случай в generic RemoteError,
    /// теряя подсказку про wire_format ровно там, где она нужнее всего —
    /// опечатка в имени метода (например, чужой регистр) выглядит именно
    /// так на стороне сервера.
    #[test]
    fn code_32000_with_method_not_found_text_is_recognized() {
        let err = from_jsonrpc_error(
            -32000,
            "method_not_found: sendMessage",
            "sendMessage",
            "sdk",
        );
        match err {
            A2aClientError::MethodNotFound {
                server_code,
                server_message,
                ..
            } => {
                assert_eq!(server_code, -32000);
                assert!(server_message.contains("method_not_found"));
            }
            other => panic!("expected MethodNotFound, got {other:?}"),
        }
    }

    /// Контрольный тест: -32000 БЕЗ маркера method_not_found — это обычная
    /// ошибка приложения (например, отказ агента), не должна маскироваться
    /// под MethodNotFound только по коду. Различие идёт по ТЕКСТУ, а не по
    /// коду, потому что -32000 шлюз использует для широкого класса ошибок,
    /// не только для несовпадения метода.
    #[test]
    fn code_32000_without_marker_stays_generic_remote_error() {
        let err = from_jsonrpc_error(
            -32000,
            "agent process crashed unexpectedly",
            "SendMessage",
            "sdk",
        );
        assert!(matches!(
            err,
            A2aClientError::RemoteError { code: -32000, .. }
        ));
    }

    #[test]
    fn code_32010_is_still_context_lost_and_not_shadowed_by_method_check() {
        let err = from_jsonrpc_error(-32010, "context expired", "SendMessage", "sdk");
        assert!(matches!(err, A2aClientError::ContextLost { .. }));
    }

    #[test]
    fn display_message_mentions_both_possible_codes() {
        let err = from_jsonrpc_error(
            -32000,
            "method_not_found: SendMessage",
            "SendMessage",
            "spec",
        );
        let text = err.to_string();
        assert!(text.contains("wire_format=spec"));
        assert!(text.contains("-32601"));
        assert!(text.contains("-32000"));
    }
}
