# Contributing Guide — agent-connector

Руководство для разработчиков, вносящих изменения в agent-connector:
рабочий процесс, структура, требования к качеству и как внести изменения.

## 1. Быстрый старт

```bash
git clone https://github.com/GG-QandV/agent-connector.git
cd agent-connector
./scripts/check.sh      # fmt + clippy + test (всё сразу)
```

Все команды определения качества:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo clippy` и `cargo test` обязаны проходить **до** пуша в `main` (см. §8). CI
(при наличии workflow) делает то же самое.

## 2. Структура workspace

```
crates/
├── adapter-model/            # DTO, идентификаторы, схемы (без runtime)
├── adapter-store-contract/   # TaskStore trait + retention policy
├── adapter-core/             # жизненный цикл задач, registry, policy
├── protocol-a2a-mapper/      # A2A <-> Core семантический маппер
├── protocol-a2a-server/      # A2A HTTP router + health/readiness
├── protocol-acp-mapper/      # ACP <-> Core маппер
├── protocol-acp-runtime/     # ACP stdio runtime
├── driver-stdio/             # UAIC/1 NDJSON subprocess driver
├── driver-http-sse/          # UAIC/1 HTTP+SSE driver
├── driver-mcp/               # MCP client driver (rmcp 0.8.5)
├── memory-task-store/        # in-memory TaskStore (tests/demo)
├── sqlite-task-store-adapter/
├── postgres-task-store-adapter/
├── adapterd/                 # composition root / daemon binary (+ config, re-exported)
└── adapterctl/               # installer / service manager CLI
```

Границы зависимостей: `adapter-core` зависит от `adapter-model` и
`adapter-store-contract`; драйверы — от `adapter-core` (trait `AgentDriver`);
`adapterd` — composition root, связывает всё. Не создавайте циклы.

## 3. Архитектурные решения — сначала ADR

Крупные/спорные решения документируются в `docs/design/adr-*.md` **до**
реализации: контекст, решение, отклонённые альтернативы. Примеры:
- `adr-0001-mcp-dynamic-capabilities.md` — hot-update, prompts/resources, input_required
- `adr-0002-mcp-server-exposure.md` — серверная сторона MCP

Правило: если решение меняет публичный контракт крейта или добавляет новую
крупную возможность — сначала короткий ADR, потом код.

## 4. Безопасность (обязательно перед каждым push)

- **Секреты только через env.** В конфиг никогда не кладётся значение
  секрета — только имя env-переменной. Проверяйте в diff: не появилось ли
  `password:`/`token:` со значением в `adapter.yaml`/доках/тестах.
- **Коммитите только нужные файлы.** Не `git add -A` в `ACP-A2A_gateway`,
  здесь — тоже стейджите только то, что относится к изменению
  (см. «Коммиты»).
- **MCP allowlist.** Не делайте `allowed_tools` optional в remote mode —
  это требование `Config::validate()`.
- **Docker ownership-лейбл.** Любая mutating-операция с ресурсами
  installer'а проверяет лейбл `io.agent-connector.managed=true`, прежде чем
  трогать чужие контейнеры/volume.

## 5. GitNexus — код через граф, не через grep

Проект индексирован GitNexus. Перед правкой любого символа:

1. `impact(target, direction="upstream", repo="agent-connector")` — blast radius.
   Если risk HIGH/CRITICAL — сначала предупредить владельца, не править вслепую.
2. `context(name)` — callers/callees для конкретного символа.
3. Перед push — `detect_changes(repo="agent-connector")`: убедиться, что
   затронуты только ожидаемые символы/флоу.

После push индексируйте: `node .gitnexus/run.cjs analyze` (из корня).
Счётчики в `AGENTS.md`/`CLAUDE.md` обновляются автоматически — коммиттите их
отдельным маленьким коммитом.

> Если `.gitnexus/run.cjs` отсутствует — `npx gitnexus analyze`. Если FTS-индекс повреждён
> («FTS index is inconsistent») — `node .gitnexus/run.cjs analyze --repair-fts`.

## 6. Тесты

### Юнит/интеграция внутри crates
- Тесты пишутся рядом с кодом (`#[cfg(test)] mod tests`) или в `tests/` крейта.
- Race-условия — `#[tokio::test(flavor = "multi_thread")]`, управляемые
  задержки/`Barrier`, реальные типы вместо моков где возможно
  (пример: `driver-mcp/tests/cancel_race.rs`, `adapter-core/tests/idempotency_race.rs`).

### Docker-зависимые тесты
Помечаются `#[ignore]` и требуют реальный Docker daemon:

```bash
cargo test -p adapterctl -- --ignored
```

Эти тесты НЕ должны падать в обычном `cargo test --workspace` — только под
флагом `--ignored`.

### Что обязательно гонять перед push в main
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Эти три команды — единственный обязательный gate перед любым push в `main`
(см. §8). Нет отдельного PR-gate — эти команды его заменяют.

## 7. Коммиты

- **Минимальные и осмысленные.** Один коммит = одно логическое изменение.
- Формат сообщений:
  - `feat(scope): ...` — новая возможность
  - `fix(scope): ...` — исправление
  - `docs(...): ...` — документация
  - `test(...): ...` — тесты
  - `chore(...): ...` — служебное (индекс, версии)
  scope: `adapterd`, `adapter-core`, `driver-mcp`, `adapterctl`, `gitnexus` и т.п.
- Стейджите только нужные файлы — никаких `Cargo.lock`-артефактов без
  причины, никаких случайных файлов.
- Секреты в diff = блокер.

## 8. Рабочий процесс и ветки

**Решение владельца репозитория (2026-08-22):** единственный активный
контрибьютор — владелец репозитория. Обязательный Pull Request для каждого
изменения даёт только дополнительные шаги без реального review вторым человеком,
поэтому обязательный PR-gate снят.

### Текущий workflow

- **Прямой push в `main` разрешён** для владельца репозитория.
  Обязательное условие: перед push пройти три команды из §6
  (`fmt --check`, `clippy -D warnings`, `test --workspace`) — они заменяют review.
- Feature-ветки (`feat/...`, `fix/...`) остаются допустимым инструментом для
  длительной/экспериментальной работы (например, многошаговые P0/P1
  интеграции протоколов), но не обязательны как шаг. Можно пушить
  напрямую в `main`, когда промежуточная ветка не нужна.
- Слияние/PR больших feature-веток в `main` — на усмотрение владельца,
  без формального требования review вторым человеком.
- Завершённые feature-ветки удаляются после того как их содержимое оказалось
  в `main` (либо через push, либо через merge).

### Неизменные правила

- `main` должен оставаться зелёным: команды из §6 обязательны перед каждым push,
  без исключений.
- Секреты в diff остаются блокером независимо от того, куда идёт push.
- Архитектурные решения (§3) всё равно фиксируются в ADR до кода — упрощённый
  git-процесс не отменяет требование фиксировать крупные решения письменно.
- `docs/anp-security-trust-policy.md` и другие binding-документы остаются
  обязательными: упрощённый git-флоу не разрешает обходить зафиксированные
  security/trust решения без нового ADR.

## 9. Релизы / версии

- Версия — единая в `Cargo.toml` (`[workspace.package] version`), наследуется
  всеми крейтами. Изменение версии = bump во всём workspace + `Cargo.lock`.
- README содержит `**Version: X.Y.Z**` — синхронизировать при bump.
- Релизный процесс (теги) — по договорённости с владельцем.

## 10. Что делать и что не делать

| ✅ Делать | ❌ Не делать |
|---|---|
| `impact()` перед правкой символа | править символ с risk HIGH/CRITICAL без предупреждения |
| `detect_changes()` перед push | коммитить вслепую, `git add -A` |
| ADR для крупных решений | молча менять публичный контракт крейта |
| стейджить только нужное | тащить секреты/случайные файлы в коммит |
| `--ignored` тесты только с Docker | фейлить workspace-тесты из-за Docker |
| индексировать GitNexus после изменений | оставлять счётчики в AGENTS.md устаревшими |
| прогонять fmt/clippy/test перед push в main | pushить без проверок из §6 |
