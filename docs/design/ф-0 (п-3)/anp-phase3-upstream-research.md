# ANP Phase 3 — Исследование upstream SDK (ответ на agent-input-guide)

> Документ отвечает на `anp-phase3-agent-input-guide.md` точными, проверяемыми фактами об upstream ANP (SDK `AgentConnect` + протокольные спецификации `AgentNetworkProtocol`). Все ссылки — на immutable commit SHA и точные пути/строки. Собран на основе локальных клонов `/tmp/anp-upstream` и `/tmp/anp-proto`.

## 1. Upstream commit и Rust crate

| Поле | Значение | Доказательство |
|---|---|---|
| SDK repo | `https://github.com/agent-network-protocol/AgentConnect` | `rust/Cargo.toml:11` `repository` |
| **Immutable commit SHA** | `aaca169c3e5b051e48875b023b60364a1dd93022` («Keep Python, Rust, and Go consumers on 0.9.4») | `git rev-parse HEAD` |
| Протокольные спеки | `https://github.com/agent-network-protocol/AgentNetworkProtocol`, HEAD `6c6aa9b8dc63b18fba228672165fc9a22aa4601f` | `git rev-parse HEAD` |
| Crate name | `anp` | `rust/Cargo.toml:2` |
| Version | `0.9.4` (git HEAD); crates.io публикует `0.9.3` (older) — на crates.io **нет** 0.9.4 | `rust/Cargo.toml:3`; crates.io API |
| Edition | `2021` | `rust/Cargo.toml:4` |
| MSRV | `1.88.0` | `rust/Cargo.toml:5` `rust-version` |
| License | `MIT` | `rust/Cargo.toml:7`; root `LICENSE` |
| Homepage / docs | `https://agent-network-protocol.com/` / `https://docs.rs/anp` | `rust/Cargo.toml:12-13` |
| Tags | Rust-крейт: простые теги `0.9.x` (без `rust/` префикса; `golang/vX.Y.Z` только для Go). Тега `0.9.4` нет — последний тег `0.9.3` | `git tag -l` |

### Cargo features (`rust/Cargo.toml:16-24`)

```toml
[features]
default = ["jwt-pem", "mls", "network"]
jwt-pem = ["dep:jsonwebtoken", "dep:pem"]
mls     = ["dep:openmls", "dep:rusqlite", ...]     # group E2EE
network = ["dep:reqwest", "dep:url"]               # WNS/DID HTTP resolution
```

Ключевые зависимости (полный список в `rust/Cargo.toml`):
`ed25519-dalek =2.1.1`, `jsonwebtoken 9`, `k256 0.13`, `p256 0.13`, `ring 0.17`, `reqwest 0.12` (optional, `rustls-tls`), `openmls =0.8.0` (optional), `x25519-dalek`, `base64`, `serde_json`, `thiserror`.

### Статус стабильности API

SDK декларирует себя как multi-language reference implementation («Shared protocol SDKs: Go and Rust cover core ANP identity/proof/WNS functionality plus selected E2EE surfaces», `README.md:30`). Версия < 1.0 (`0.9.x`), API может меняться. **Immutable point для сборки — commit `aaca169c`**, не версия на crates.io.

### Минимальный `Cargo.toml` example

```toml
[package]
name = "anp-min-client"
version = "0.1.0"
edition = "2021"
rust-version = "1.88"

[dependencies]
anp = { git = "https://github.com/agent-network-protocol/AgentConnect", rev = "aaca169c3e5b051e48875b023b60364a1dd93022", default-features = true }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
serde_json = "1"
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }
```

> Если нужен только identity+auth+WNS (без group E2EE): `default-features = false, features = ["jwt-pem", "network"]` — заметно меньше сборки (без openmls/rusqlite).

### Команда сборки minimal example

```bash
cargo build --release --example wns_example            # существующий upstream example
cargo run --example wns_example                        # офлайн: validate_handle + build_resolution_url
cargo test --test wns_tests                            # офлайн unit-тесты WNS/DID
```

`cargo run --example wns_example` не требует сети и не требует E2EE/MLS.

---

## 2. Runnable minimal Rust client

### Что SDK умеет «из коробки» (доказано по `src/`)

- Создание DID WBA документа и ключей: `create_did_wba_document(hostname, DidDocumentOptions)` → `DidDocumentBundle { did_document, keys: BTreeMap<method_fragment, GeneratedKeyPairPem> }` (`src/authentication/did_wba.rs:410,526`).
- `PrivateKeyMaterial::{Secp256k1, Secp256r1, Ed25519, X25519}`, `sign_message`/`verify_message`, PEM `to_pem`/`from_pem`/`from_compatible_private_pem` (`src/keys.rs:24-150`).
- Legacy DID-WBA auth header: `generate_auth_header(generate_auth_payload(...))` — подписывает canonical JSON `{nonce, timestamp (%Y-%m-%dT%H:%M:%SZ), domain, did}`; SHA-256 как signing input (`src/authentication/did_wba.rs`).
- Современная HTTP Message Signature: `generate_http_signature_headers(did_document, request_url, method, private_key, headers, body, HttpSignatureOptions)` → `{Signature, Signature-Input, Content-Digest}` (`src/authentication/http_signatures.rs:97-121`).
- DID document resolution (network): `resolve_did_document`/`resolve_did_document_with_options`/`resolve_did_document_sync` (`src/authentication/did_resolver.rs:18-118`).
- WNS: `resolve_handle`, `resolve_handle_sync`, `build_resolution_url`, `build_wba_uri`, `parse_wba_uri`, `validate_handle` (`src/wns/resolver.rs`, `src/wns/models.rs`).
- Direct E2EE (X3DH-like): `DirectE2eeSession::initiate_session`/`accept_incoming_init`/`encrypt_follow_up`/`decrypt_follow_up`; prekey bundle `build_prekey_bundle`/`verify_prekey_bundle`; envelope helpers (`src/direct_e2ee/*`).

### Критично: SDK **не** является task-delegation клиентом

- В SDK нет ни `invoke`, ни `status`, ни `cancel`, ни `stream` remote-задачи.
- SDK README прямо говорит: JSON-объект приложения трактуется как **opaque application payload**; «command, status, task, result... определяются вызывающим кодом выше уровня ANP SDK».
- SDK генерирует **транспортно-нейтральные** `{method, params}` объекты (например `direct_send_request(...)` → `{"method": "direct.send", "params": {...}}`, `src/direct_e2ee/envelope.rs:255-271`), а **полный JSON-RPC envelope** (`jsonrpc`, `id`) строит вызывающий слой (документировано в `docs/e2e/direct-e2ee-p5-sdk.md:120`).
- Протокольный уровень: ANP = **messaging/RPC**, не task-протокол. Методы: `direct.send`, `direct.incoming` (notification), `group.send`, `group.e2ee.send`, `anp.get_capabilities`, `anp.negotiate`, attachments `attachment.*` (`AgentNetworkProtocol/message/01-core-binding.md`, `03-...`, `06-...`).

### Рабочий minimal client (identity + auth header, офлайн)

```rust
use anp::authentication::{create_did_wba_document, generate_http_signature_headers, DidDocumentOptions};
use anp::PrivateKeyMaterial;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = create_did_wba_document(
        "example.com",
        DidDocumentOptions::default().with_path_segments(["agents", "alice"]),
    )?;
    let did = bundle.did_document["id"].as_str().unwrap().to_string();
    let key = PrivateKeyMaterial::from_pem(&bundle.keys["key-1"].private_key_pem)?;

    let headers = generate_http_signature_headers(
        &bundle.did_document,
        "https://peer.example/auth",
        "POST",
        &key,
        None,
        Some(br#"{"jsonrpc":"2.0","id":1,"method":"anp.get_capabilities","params":{}}"#),
        Default::default(),
    )?;
    println!("DID: {did}\nHeaders: {headers:?}");
    Ok(())
}
```

---

## 3. Local/test peer setup

Публичных production-эндпоинтов ANP нет. Upstream предоставляет **локальные reference peers**:

### A. Python auth server (используется в официальных Rust interop-тестах)

`examples/python/rust_interop_examples/python_auth_server.py` — минимальный HTTP-сервер, который: отдаёт DID-документ по пути `.well-known`/path-DID, проверяет HTTP Signature через `DidWbaVerifier`, выдаёт Bearer-токен (HS256, `--jwt-secret`).

Запуск (точная команда из `rust/tests/python_network_interop_tests.rs:154-177`):

```bash
uv run --python 3.13 --with-editable /tmp/anp-upstream \
  python /tmp/anp-upstream/examples/python/rust_interop_examples/python_auth_server.py \
  --did-json /tmp/peer/did.json --port 18080 --jwt-secret test-secret
```

Rust-клиент против этого сервера: `rust/tests/python_network_interop_tests.rs` (`test_rust_http_client_to_python_server` — HTTP Signatures, затем Bearer).

### B. Rust standalone fixture (E2EE, полностью офлайн)

`rust/examples/direct_e2ee_interop_cli.rs` — генерирует полный E2EE handshake Alice↔Bob (DID docs, prekey bundle, init body, cipher follow-up) в JSON. Запуск:

```bash
cd /tmp/anp-upstream/rust && cargo run --example direct_e2ee_interop_cli -- fixture
```

### C. DID creation (локальный identity без сети)

```bash
cd /tmp/anp-upstream/rust && cargo run --example create_did_document
```

Создаёт `did:wba:example.com:user:...` + PEM-ключи; приватные ключи сохраняются в local dir.

### D. Тестовый DID / endpoint для integration-тестов

- `did:wba:example.com:user:alice` — используется в `rust/tests/wns_tests.rs:36-65` (локальная mock-раздача `/.well-known/handle/alice`).
- Для наших integration-тестов: поднимаем **собственный** minimal ANP peer на `127.0.0.1:<port>` (pattern из interop-тестов), DID создаётся через `create_did_wba_document`, WNS/DID resolution направляем на локальный mock-сервер (как в `wns_tests.rs`). Без реального публичного WNS-сервиса.

**Вывод:** готового публичного test DID/endpoint нет; локальный reference peer строится из upstream-примеров (auth-сервер / interop fixture). Это удовлетворяет критерию «independent local or remote ANP peer for integration tests».

---

## 4. Discovery и DID trust flow

### Как получить endpoint peer

| Механизм | API / формат | Источник |
|---|---|---|
| Static URL | клиент передаёт `https://host/...` напрямую | — |
| DID resolution | `resolve_did_wba_document(did)` / `resolve_did_document*` → DID document, из `service[].ANPMessageService.serviceEndpoint` | `src/authentication/did_resolver.rs` |
| WNS handle | `resolve_handle("alice.example.com")` → `HandleResolutionDocument { handle, did, status, binding_generation, versionId, ttl, profile }` → затем resolve DID | `src/wns/resolver.rs`, `src/wns/models.rs` |
| WNS URL | `https://{domain}/.well-known/handle/{local_part}` | `src/wns/resolver.rs` `build_resolution_url` |
| Handle URI | `wba://{local}.{domain}` (RFC-ish), `parse_wba_uri`/`build_wba_uri` | `rust/examples/wns_example.rs` |

`HandleServiceEntry { id, type, serviceEndpoint }` — сервисы агента (`src/wns/models.rs`).

### Формат DID и DID document

- DID: `did:wba:{hostname}:{path_segments...}:{fragment}` (path-based, default), либо `did:wba:{hostname}:e1_<fingerprint>` (e1_ profile) / `k1_` profile (`docs/did/getting_started.md`).
- DID document (JSON-LD): `id`, `verificationMethod[]` (ключи `key-1` signing, `key-3` X25519 static), `authentication[]`, `service[]` (в т.ч. `ANPMessageService`), опционально `proof`.
- **Default DID profile** на HEAD: `e1_` (path-based создаётся как `e1_`), `create_did_wba_document_with_key_binding` deprecated → `create_did_wba_document(..., profile)` (`docs/did/getting_started.md`).

### Связь DID → endpoint → verification key

1. Resolve DID document (HTTPS, через DID method).
2. Взять `service[ANPMessageService].serviceEndpoint` → endpoint.
3. Для auth: KID (фрагмент `#key-1`) **обязан** быть в `authentication` relationship DID-документа; `assertionMethod`/`keyAgreement`/`verificationMethod` → `HttpSignatureError::VerificationMethodNotAuthorizedForAuthentication` (`src/authentication/http_signatures.rs:64-66`; протокол: `AgentNetworkProtocol/message/02-identity-and-discovery.md:188-199`).
4. Проверка подписи — через public key из DID document (verification key), привязанного к `keyid`.

### Алгоритмы / keys

- Подписи: `Secp256k1`, `Secp256r1`, `Ed25519` (`src/keys.rs`).
- Key agreement: `X25519` (static + signed prekey), X3DH-like.
- JWT-оверлей в примерах: `HS256` (interop), поддержка RS256 (`docs/ap2/ES256K_SUPPORT.md`).

### Rotation / revocation

- **Prekey rotation**: `SignedPrekey { key_id, ... }` в `PrekeyBundle`; `signed_prekey_from_private_key`; receiver подтверждает `signed_prekey.key_id` и `key_id` подписывающего ключа (`src/direct_e2ee/bundle.rs`, `src/direct_e2ee/models.rs`).
- **DID update/rotation**: определяется DID method (`did:wba`); документ может менять verificationMethod/authentication. Проверка актуальности — responsibility клиента через пере-resolve (для production).
- **WNS binding**: `binding_generation` + `versionId` в `HandleResolutionDocument` — маркеры смены привязки handle→DID (`src/wns/models.rs`).

### Trust model

- TOFU **не принимается** (как и требует guide). Для production: peer DID документ резолвится по HTTPS через DID method + верификация binding (`e1_`/`k1_`), а не «первый увиденный ключ». Для локальных тестов — pinned локальный DID document (mock-раздача), как в `wns_tests.rs`.

---

## 5. Invoke / status / cancel / input API

**Главный ответ guide:** в SDK **нет** операций invoke/status/cancel/input на уровне remote task. SDK даёт только transport-neutral `{method, params}` объекты и crypto/identity. Поэтому все поля таблицы ниже определяются **прикладным слоем поверх `direct.send`** — и на текущем upstream (без дополнительной спецификации) эти операции **не реализованы**.

| Операция | Статус в upstream | Обязательные данные |
|---|---|---|
| Invoke | Нет API. Ближайшее: `direct_send_request(local_did, peer_did, operation_id, message_id, content_type, body)` → `{method:"direct.send", params}`; JSON-RPC envelope строим сами | remote agent/skill, input, context, deadline, idempotency key — всё как application payload внутри `body`/`meta` |
| Status | Нет API. Статус — application semantics внутри сообщений | нет protocol-level status states |
| Cancel | Нет API. Единственный «отменный» механизм протокола — `operation_id` dedup (см. ниже) | — |
| Provide input | Нет API. Mid-call input не предусмотрен ANP (сообщения однонаправленные) | — |
| Capabilities | Протокольный метод `anp.get_capabilities` (в SDK **нет** функции; сервер реализует сам). Ответ: `security_profiles`, `auth_schemes`, `profiles`, `accepts`, `priority` (`AgentNetworkProtocol/message/01-core-binding.md:395-443`) | — |

### Idempotency: повторный invoke с тем же key — **новая задача или нет?**

Правило `direct.send` (`AgentNetworkProtocol/message/03-direct-messaging-base-semantics.md:415-467`):

- Idempotence judgement по минимальному набору: `(sender_did, target.did, method, operation_id)`. Если запись существует → вернуть уже принятый результат (`accepted=true`), **не создавая новую** remote task.
- Затем проверяется `message_id` (duplicate → dedup).
- Успех `direct.send` = **только** «ingress принял сообщение», не «задача выполнена» (§3.3, §8.1).
- **Вывод для driver:** повторный invoke с тем же idempotency key **не создаёт новую remote task** — возвращается результат прежнего принятого сообщения. Это воспроизводимо и пригодно для `idempotency_key` в AdapterCore.

> Важно: это idempotency на уровне **доставки сообщения**, а не завершения задачи. Для task-семантики (terminal state, результаты) upstream не даёт контракта — driver должен либо моделировать это сам (сообщения-ответы), либо признать gap (см. §10).

---

## 6. Stream event schema и resume contract

**Блокер подтверждён:** upstream **не даёт** stable cursor + catch-up history для remote task.

Факты:

- ANP — request/response JSON-RPC 2.0 поверх HTTPS/WSS + Notifications (`AgentNetworkProtocol/message/01-core-binding.md:56-133`). Batch запрещён (`anp.batch_not_supported`, §5.4).
- Push-механизм — **Notifications**: `direct.incoming` — «стандартный push-Notification», которым endpoint доставляет принятое сообщение целевому агенту (`03-direct-messaging-base-semantics.md:374-393`).
- **Нет** `event_id`, `seq`/cursor, `Last-Event-ID`, `after_seq`, retention/expiry, reconnect-контракта «после cursor».
- Terminal-состояния (`Completed`/`Failed`/`Cancelled`) на protocol-level **не определены** — это прикладной payload.
- Keepalive/idle — ответственность транспорта (HTTP/WSS), SDK не декларирует.

### Что это значит для driver-anp-client

Required example из guide **не реализуем** на чистом upstream:

```text
invoke → remote_task_id        # нет remote task ID из direct.send (только accepted=true)
stream → seq=1 accepted        # нет seq
<network disconnect>
stream(after_seq=2) → ...      # нет resume cursor
```

**Решение (как и предусмотрено guide):** поток помечается как **non-resumable**; reconnect не обещает отсутствия потерь. Возможна лишь компенсация на уровне прикладного протокола (retry `direct.send` с тем же `operation_id`, idempotent на стороне приёмника), но не строгий event replay.

---

## 7. Error / retry table

### Error enums upstream (`src/`)

- **WNS** (`src/wns/errors.rs`): `WnsError{message,status_code}`, `HandleValidationError`, `HandleNotFoundError`, `HandleGoneError`, `HandleMovedError{redirect_url}`, `HandleResolutionError`, `HandleBindingError`, `WbaUriParseError` — все с `status_code` (HTTP).
- **HTTP Message Signature** (`src/authentication/http_signatures.rs:51-75`): `MissingSignatureInputOrSignatureHeader`, `InvalidSignatureHeaderFormat`, `InvalidSignatureInput`, `MissingContentDigestHeader`, `ContentDigestVerificationFailed`, `VerificationMethodNotFound`, `InvalidOrForeignVerificationMethodKid`, `VerificationMethodNotAuthorizedForAuthentication`, `SignatureVerificationFailed`, `SigningFailed`.
- **Direct E2EE** (`src/direct_e2ee/errors.rs:6-23`): `UnsupportedSuite`, `MissingField`, `InvalidField`, `ProofError`, `CanonicalJsonError`, `CryptoError`, `SessionNotFound`, `PendingOutboundNotFound`, `ReplayDetected`.
- **JSON-RPC протокольные ошибки** (`AgentNetworkProtocol/message/01-core-binding.md`): `anp.invalid_request_id`, `anp.batch_not_supported` и стандартные JSON-RPC 2.0.

### Маппинг в категории guide

| Категория guide | Upstream errors | Retryable? | Retry-after? |
|---|---|---|---|
| `identity_untrusted` | `SignatureVerificationFailed`, `VerificationMethodNotAuthorizedForAuthentication`, `InvalidOrForeignVerificationMethodKid`, `WnsError`/`HandleBindingError` (binding mismatch) | **Нет** (небезопасно) | Нет |
| `authorization` | 401/403 от HTTP Signature/Bearer (interop server), `AuthenticationError` | Нет (кроме ротации ключа) | Нет |
| `unsupported_capability` | протокольный `method not found` / `anp.get_capabilities` не содержит нужного | Нет | Нет |
| `rate_limited` | HTTP 429 (проксируется через `status_code` у WNS-ошибок) | **Да** (с backoff) | Может быть (`Retry-After`) |
| `transport_failure` | reqwest/network errors, `WnsError` с 5xx | **Да** (с backoff) | По HTTP |
| `protocol_failure` | `anp.invalid_request_id`, `anp.batch_not_supported`, `InvalidSignatureInput`, `CanonicalJsonError` | Нет (баг клиента) | Нет |
| `stream_gap` | не существует в upstream (нет seq) | — | — |
| `resume_unavailable` | не существует в upstream (нет cursor) | — | — |
| `remote_task_not_found` | не существует (нет task ID в протоколе) | — | — |

**Retry invoke:** безопасен только с тем же `operation_id` (idempotent на приёмнике). С новым `operation_id` — это новое сообщение.

---

## 8. Capability / version negotiation

- **`anp.get_capabilities`** — протокольный метод; в SDK функции нет, реализуется сервером/клиентом как JSON-RPC. Ответ статически заявляет: `security_profiles` (`transport-protected`, `direct-e2ee`, `group-e2ee`), `auth_schemes`, `profiles`, `accepts`, `priority` (`AgentNetworkProtocol/message/01-core-binding.md:395-443`). На HEAD SDK default-профиль identity — `e1_`/`k1_`.
- **`anp.negotiate`** (meta-protocol, `AgentNetworkProtocol/06-...`): семантическая переговорка интерфейса/Profile/security-profile/schema. Возвращает `execution_modes`: `direct_structured_call`, `direct_message`, `group_message`, `async_task`, `stream`, `natural_language`, `natural_language_protocol_drafting` (§8). Результат — `{ selected, execution, ... }` с `negotiationId`, `validUntil`, `negotiationDigest` (caching).
- **Versioning**: явного «protocol version» в виде числа в SDK нет; профили именованы (`anp.direct.e2ee.v1`, `anp.core.binding.v1`). Приоритет при конфликте статических подсказок: DID document → `anp.get_capabilities` runtime → refresh cache (`06-...:118-129`).

---

## 9. Security / key-management constraints

- **Transport**: HTTPS (или WSS) обязателен (binding «MUST run over a certified secure transport», `01-core-binding.md:123-126`). Локальные тесты — HTTP на `127.0.0.1`.
- **Auth-схемы**:
  - Современная: HTTP Message Signatures (`Signature`, `Signature-Input`, `Content-Digest`) — default (`docs/did/getting_started.md`).
  - Legacy: `Authorization: DIDWba ...` (`generate_auth_header`) — SHA-256 canonical JSON `{nonce, timestamp, domain, did}`.
  - Bearer (JWT) — поверх (interop).
- **Replay protection**: nonce (16 байт base64url) + timestamp в legacy auth; HTTP Message Signature подпись покрывает `content-digest` и method/target; в E2EE — `DirectE2eeError::ReplayDetected` (ratchet skip/chain check).
- **KID constraint**: ключ, подписавший запрос, обязан быть в `authentication` relationship DID-документа — не `assertionMethod`/`keyAgreement` (`http_signatures.rs:64-66`).
- **Хранение ключей**: SDK отдаёт PEM (`GeneratedKeyPairPem`), `PrivateKeyMaterial::from_pem`/`to_pem`. Собственного secure storage/kms **нет** — хранилище (env/файл/secret manager) — ответственность driver. Debug-вывод маскирует приватные ключи (`src/keys.rs:31-41`).
- **Тестовые ключи**: только local fixture (примеры upstream генерируют свои при каждом запуске), никогда для production.

---

## 10. Known gaps vs P0 spec

| # | P0 требование | Upstream статус | Impact / решение |
|---|---|---|---|
| 1 | Remote task lifecycle (`invoke/status/cancel`) | **Нет в SDK и протоколе** | Driver не может предоставить AgentDriver API напрямую. Нужен прикладной контракт поверх `direct.send` (или исключение lifecycle из P0) |
| 2 | Remote task ID в ответе invoke | Нет (ответ `direct.send` = `accepted=true`, без task ID) | AdapterCore остаётся владельцем TaskId; remote correlation — через `operation_id`/`message_id` |
| 3 | Idempotent invoke | **Есть** (по `(sender_did,target.did,method,operation_id)` + `message_id` dedup) | Реализуемо; повторный invoke с тем же key не создаёт новую задачу |
| 4 | Stream с stable cursor | **Нет** (нет seq/event_id/retention) | Поток **non-resumable**; guide это допускает, но надо явно зафиксировать |
| 5 | Provide input (mid-call) | Нет | Не поддерживается → как в driver-mcp: `CoreError::InvalidRequest` |
| 6 | Capabilities (streaming/resume/cancel/input/artifacts/protocol version) | Только `anp.get_capabilities` (security_profiles/auth_schemes/profiles/accepts) + `anp.negotiate`; реализуется нами | Маппинг на `AgentDriver.capabilities()` ручной |
| 7 | Verified peer identity (trust) | DID resolution + `authentication` KID check — **есть**; TOFU не принимается | Реализуемо для production |
| 8 | Independent local peer для integration-тестов | Есть: python auth server + interop fixtures | Реализуемо |
| 9 | `direct.send` / `direct.incoming` messaging | **Есть** в SDK (`direct_e2ee/envelope.rs`, `session.rs`) | Базис для любого прикладного контракта |
| 10 | E2EE (`require_e2ee: true`) | X3DH-like + prekey bundles + ratchet — есть | Реализуемо; требует prekey-публикацию через endpoint |

### Итог по готовности к real SDK adapter (критерии guide)

| Критерий guide | Статус |
|---|---|
| Immutable SDK revision + воспроизводимая сборка | ✅ commit `aaca169c`, MSRV 1.88, `cargo run --example wns_example` |
| Verified peer identity | ✅ DID resolution + `authentication` KID check (нет TOFU) |
| Idempotent invoke | ✅ `(sender_did,target.did,method,operation_id)` + `message_id` dedup |
| Cancel и status | ❌ нет в протоколе |
| Stream с stable cursor | ❌ нет (non-resumable) |
| Documented resume/catch-up | ❌ нет |
| Independent ANP peer для integration tests | ✅ локальный peer из upstream-примеров |

**Вывод:** условия 4/5/6 (cancel, status, stream cursor, resume) на чистом upstream **не выполняются**. Это значит, что полноценный `AgentDriver`-интерфейс поверх ANP реализовать нельзя без прикладного контракта поверх `direct.send` (или исключения этих claims из P0 и ограничения P0 scope identity/discovery/messaging). Согласуйте это до перехода к real SDK adapter.

---

## Приложение: один working invoke example + один reconnect/resume example

### Working invoke (сообщение с idempotency key)

Запрос (JSON-RPC envelope строим сами поверх `direct.send`):

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "direct.send",
  "params": {
    "meta": {
      "sender_did": "did:wba:example.com:agents:alice:...",
      "target": { "kind": "agent", "did": "did:wba:example.com:agents:bob:..." },
      "operation_id": "op-<idempotency_key>",
      "message_id": "msg-<uuid>",
      "content_type": "application/json",
      "profile": "anp.direct.e2ee.v1",
      "security_profile": "direct-e2ee"
    },
    "body": { "application_content_type": "application/json", "payload": { "action": "invoke", "input": {...} } }
  }
}
```

Ответ: `accepted = true` (или ошибка по §7). Повторный запрос с тем же `operation_id` → тот же `accepted=true`, **без новой задачи**.

### Reconnect/resume

**Нет supported resume API.** Минимальный воспроизводимый сценарий из guide не реализуем (нет seq/cursor). Единственное, что можно сделать на upstream: retry `direct.send` с тем же `operation_id` — приёмник дедуплицирует. Для event replay поверх задачи — контракт отсутствует, поток помечается **non-resumable**.

---

## Проверяемость

- Локальные клоны: SDK `/tmp/anp-upstream` (HEAD `aaca169c3e5b051e48875b023b60364a1dd93022`), протокол `/tmp/anp-proto` (HEAD `6c6aa9b8dc63b18fba228672165fc9a22aa4601f`).
- Ключевые upstream paths:
  - `rust/Cargo.toml` — version/features/MSRV/license.
  - `rust/src/keys.rs` — PrivateKeyMaterial/PublicKeyMaterial.
  - `rust/src/authentication/http_signatures.rs:97-121` — generate_http_signature_headers; `:51-75` — HttpSignatureError.
  - `rust/src/authentication/did_wba.rs:410,526` — DidDocumentBundle/create_did_wba_document; `:487` — AuthenticationError.
  - `rust/src/wns/{resolver,models,errors}.rs` — WNS.
  - `rust/src/direct_e2ee/{session,envelope,bundle,errors}.rs` — E2EE.
  - `AgentNetworkProtocol/message/01-core-binding.md` — JSON-RPC binding, `anp.get_capabilities`, notifications, batch-ban.
  - `AgentNetworkProtocol/message/03-direct-messaging-base-semantics.md:374-467` — `direct.incoming`, idempotency `(sender,target,method,operation_id)`.
  - `AgentNetworkProtocol/06-anp-agent-communication-meta-protocol-specification.md` — `anp.negotiate`, execution_modes.