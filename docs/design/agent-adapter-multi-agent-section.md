## 19. Несколько локальных агентов: общий Adapter Daemon

### 19.1 Решение по умолчанию

Если на одной машине, в одном контейнере или в соседних контейнерах работают 2–3 и более агентов, по умолчанию используется **один общий Adapter Daemon**.

```text
Agent A ──┐
Agent B ──┼── stdio / Unix socket / localhost HTTP ──► Adapter Daemon
Agent C ──┘                                              │
                                                        ├─ A2A client → remote agents
                                                        └─ A2A server ← external callers
```

Каждый агент остаётся отдельным процессом или контейнером. Adapter не объединяет их внутреннюю логику и не делает их одним агентом. Он даёт им общие:

- A2A/ACP compatibility endpoints;
- task/session/event runtime;
- auth и policy;
- transport discovery;
- observability;
- scheduler и resource limits.

### 19.2 Зачем один adapter

Один daemon предпочтительнее отдельных adapter-процессов для каждого локального агента, потому что:

- один внешний A2A endpoint вместо нескольких портов;
- один policy/security boundary для доверенной локальной группы агентов;
- один `TaskRegistry`, `SessionRegistry` и durable event journal;
- единые quotas и backpressure для всей машины;
- один механизм discovery и transport fallback;
- локальные агенты могут делегировать задачи друг другу через core без сетевого round-trip;
- меньше процессов, конфигурации, ключей и operational overhead.

### 19.3 AgentRegistry

Добавить в `adapter-core` отдельный реестр агентов. Он не заменяет `TaskRegistry` и `SessionRegistry`.

```text
AgentRegistry   — какой агент доступен, какие у него skills, connector, health и limits.
TaskRegistry    — какая задача выполняется, её state, owner, события и result.
SessionRegistry — к какому логическому диалогу/контексту относится задача.
```

Минимальная модель:

```rust
pub struct RegisteredAgent {
    pub id: AgentId,
    pub descriptor: AgentDescriptor,
    pub connector: Arc<dyn AgentConnector>,
    pub skills: Vec<SkillDescriptor>,
    pub limits: AgentLimits,
    pub health: HealthState,
    pub policy: AgentPolicy,
}

pub trait AgentRegistry: Send + Sync {
    async fn register(&self, agent: RegisteredAgent) -> Result<(), RegistryError>;
    async fn get(&self, id: &AgentId) -> Result<Option<RegisteredAgent>, RegistryError>;
    async fn find_by_skill(
        &self,
        query: &SkillQuery,
        caller: &Caller,
    ) -> Result<Vec<AgentCandidate>, RegistryError>;
    async fn mark_health(&self, id: &AgentId, health: HealthState)
        -> Result<(), RegistryError>;
}
```

### 19.4 Конфигурация нескольких агентов

```yaml
mode: local

agents:
  - id: reviewer
    name: Code Reviewer
    transport: stdio
    command: ./bin/reviewer-agent
    skills:
      - id: code-review
        tags: [code, review, security]
    limits:
      max_concurrent_tasks: 2

  - id: docs-agent
    name: Documentation Agent
    transport: unix-socket
    socket: /run/docs-agent.sock
    skills:
      - id: docs-search
        tags: [documentation, retrieval]
    limits:
      max_concurrent_tasks: 4

  - id: test-agent
    name: Test Agent
    transport: http-sse
    endpoint: http://127.0.0.1:8088
    skills:
      - id: test-run
        tags: [tests, qa]
    limits:
      max_concurrent_tasks: 1
```

Каждый элемент `agents[]` создаёт отдельный `AgentConnector`. Новая реализация connector добавляется без изменения `TaskManager` или A2A/ACP mappings.

### 19.5 Маршрутизация задач

Добавить `SkillRouter` как отдельный модуль между protocol adapter и `TaskManager`.

```text
Incoming A2A/ACP task
        │
        ▼
  Authorization policy
        │
        ▼
     SkillRouter
        │
        ├─ explicit agent_id → указанный агент
        ├─ explicit skill_id → агент-владелец skill
        └─ capability match → лучший eligible agent
        │
        ▼
 TaskManager + выбранный AgentConnector
```

Порядок выбора:

1. Если caller явно указал `agent_id` и имеет право его вызывать — использовать его.
2. Если указан `skill_id` — выбрать агента, который объявил skill.
3. Если указан только capability/tag — выбрать здорового кандидата по policy, priority и свободной capacity.
4. Если кандидатов нет — вернуть явную ошибку `NoEligibleAgent`; не отправлять задачу случайному агенту.

Router не использует LLM для выбора в MVP. Это должен быть детерминированный policy-based выбор. LLM-routing может быть отдельным future module, но не частью compatibility runtime.

### 19.6 A2A client и A2A server — отдельные роли

У adapter должны быть независимо включаемые роли:

```text
A2A Client role:
  local agent / core → discover remote Agent Card → invoke remote task → consume events

A2A Server role:
  external caller → Adapter → authorize → route to local agent → stream task events
```

Минимальная безопасная настройка для локальной машины, где агентам нужно только вызывать удалённых агентов:

```yaml
a2a:
  client: true
  server: false
```

В таком режиме adapter не публикует локальные агенты во внешнюю сеть и не открывает входящий listener. Он выполняет только исходящие A2A-вызовы.

Если другие агенты должны вызывать локальных исполнителей:

```yaml
a2a:
  client: true
  server: true
```

Для `server: true` в local profile listener по умолчанию ограничивается `127.0.0.1` или Unix socket. Публикация наружу допускается только в remote profile через HTTPS reverse proxy, mTLS или outbound remote-connect tunnel.

### 19.7 Локальная агент-агент делегация

Когда один зарегистрированный local agent должен вызвать другой зарегистрированный local agent, не нужно делать HTTP/A2A round-trip.

```text
Reviewer Agent
      │ CoreCommand::Invoke(target = test-agent)
      ▼
Adapter Core → SkillRouter → TestAgentConnector → Test Agent
```

Core сохраняет обычный `Task`, события и policy как для внешнего A2A-вызова. Поэтому audit, cancellation, limits и resume работают одинаково.

A2A serialisation применяется только на внешней границе:

```text
external agent ⇄ A2A wire protocol ⇄ Adapter
local agent    ⇄ normalized CoreCommand/Event ⇄ Adapter
```

Это уменьшает задержку, исключает лишнюю сериализацию и сохраняет transport-neutral core.

### 19.8 Лимиты и изоляция внутри общего daemon

Общий daemon не означает отсутствие изоляции. Обязательные механизмы:

- `max_concurrent_tasks` на каждого агента;
- отдельный bounded queue на каждого агента;
- отдельный timeout/deadline policy;
- circuit breaker при repeated connector failures;
- restart supervision для stdio subprocess;
- healthcheck и временное исключение unhealthy agent из router;
- per-agent access policy: caller может иметь доступ не ко всем локальным agents/skills;
- максимальный размер input/output/artifact metadata.

Отказ одного агента не должен останавливать daemon или отменять задачи других агентов.

### 19.9 Когда нужен отдельный adapter на агента

Отдельные adapter instances оправданы, если требуется хотя бы одно из условий:

- разные tenant/владельцы;
- недоверенные агенты;
- отдельные secrets и security policies;
- независимое горизонтальное масштабирование;
- отдельный публичный A2A identity/endpoint на каждого агента;
- разные release cycles;
- агент потребляет ресурсы так, что его нужно изолировать на уровне container/host.

Во всех остальных случаях default — один Adapter Daemon и много `RegisteredAgent`.

### 19.10 Модель расширения

Multi-agent поддержка не требует rewrite MVP. Она добавляется последовательно:

1. В MVP есть один `RegisteredAgent` и один connector.
2. Добавляется `AgentRegistry` и `SkillRouter`; одиночный агент становится записью в registry.
3. Добавляется несколько connector instances и per-agent limits.
4. Добавляется optional A2A client role для outgoing delegation.
5. Добавляется optional A2A server role для входящих внешних задач.
6. В remote/multi-instance deployment registry metadata и policies переносятся в Postgres/control plane, а task ownership остаётся через leases.

`TaskManager`, task state machine, event journal и transport abstraction на этих этапах не меняются.
