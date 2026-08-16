// ФИКС 5 (завершение фикса 2): per-request Caller через SDK ExecutorContext.
//
// Подтверждено чтением реального sdk_a2a_server_handler.rs (a2a-server crate,
// pinned commit 02ee5602...):
//
//   pub struct ExecutorContext {
//       pub message: Option<Message>,
//       pub task_id: TaskId,
//       pub stored_task: Option<Task>,
//       pub context_id: String,
//       pub metadata: Option<Value>,
//       pub user: /* тип не показан в тесте, но поле явно существует */,
//       pub service_params: ServiceParams,
//       pub tenant: Option<String>,
//   }
//
// И критично важная деталь из start_execution():
//
//   let exec_ctx = crate::ExecutorContext {
//       message: Some(req.message),
//       task_id: task_id.clone(),
//       stored_task: ...,
//       context_id,
//       metadata: req.metadata,
//       user: None,                    // <-- SDK САМ не заполняет user!
//       service_params: params.clone(),
//       tenant: req.tenant,
//   };
//
// ВАЖНЫЙ ВЫВОД: DefaultRequestHandler::start_execution() ВСЕГДА кладёт
// user: None в ExecutorContext, независимо от того, что было в HTTP request.
// Значит SDK не резолвит authenticated user сам — это ответственность
// wire-layer (JSON-RPC/REST router), который получает params: &ServiceParams
// в КАЖДОМ методе RequestHandler trait (send_message, cancel_task, и т.д.)
// ДО того, как SDK строит ExecutorContext.
//
// Значит caller identity должна попадать через ServiceParams, не через
// ExecutorContext.user напрямую (тот остаётся None в стандартном потоке).
// Нужно посмотреть определение ServiceParams (crate::middleware::ServiceParams)
// — оно упомянуто в use crate::middleware::ServiceParams, но не показано в
// этом файле. Если ServiceParams::new() создаётся пустым (как в тестах:
// ServiceParams::new()), и если у него есть builder/setter для extensions
// или headers, ИМЕННО ТАМ должен передаваться resolved Caller от нашего
// auth middleware к RequestHandler impl.
//
// ============================================================
// ЧАСТЬ 1: наш RequestHandler impl читает Caller из ServiceParams
// ============================================================
//
// НЕ ФИНАЛЬНЫЙ КОД — требует точного определения ServiceParams, которого
// у меня нет. Но архитектурная схема правильная и confirmed структурой SDK:
//
// use a2a_server::{RequestHandler, ServiceParams, ExecutorContext};
//
// pub struct AdapterRequestHandler {
//     core: Arc<AdapterCore>,
//     inner_executor: Arc<AdapterAgentExecutor>,  // остаётся как есть
// }
//
// #[async_trait]
// impl RequestHandler for AdapterRequestHandler {
//     async fn send_message(&self, params: &ServiceParams, req: SendMessageRequest)
//         -> Result<SendMessageResponse, A2AError>
//     {
//         // ЗДЕСЬ извлекаем Caller из params (нужен точный API ServiceParams,
//         // вероятно params.extensions().get::<Caller>() или аналог, если
//         // ServiceParams поддерживает generic extensions map, как это принято
//         // в axum-style middleware chains).
//         let caller = extract_caller_from_params(params)?;
//         // ... остальная реализация проксирует в DefaultRequestHandler или
//         // напрямую в AdapterCore, используя РЕЗОЛВЕННОГО caller, а не
//         // статическую строку.
//     }
//     // ... остальные методы аналогично
// }

// ============================================================
// ЧАСТЬ 2: ЕСЛИ ServiceParams не поддерживает extensions map — план Б
// ============================================================
//
// Альтернативный, более грубый, но рабочий путь: не оборачивать
// DefaultRequestHandler своим RequestHandler impl, а вместо этого
// реализовать AgentExecutor так, чтобы caller передавался через боковой
// канал, синхронизированный по task_id/context_id — например,
// thread-local или Arc<DashMap<TaskId, Caller>>, заполняемый Axum
// middleware ДО того, как запрос попадёт в DefaultRequestHandler, и
// читаемый нашим AdapterAgentExecutor::execute() по ctx.task_id.
//
// Это менее чисто (side-channel state), но не требует переопределения
// всего RequestHandler trait (10+ методов) только чтобы прокинуть 1 поле.
//
// pub struct AdapterAgentExecutor {
//     core: Arc<AdapterCore>,
//     pending_callers: Arc<DashMap<String, Caller>>,  // task_id -> Caller
//     default_caller: Caller,  // fallback для случаев без auth middleware
// }
//
// impl AdapterAgentExecutor {
//     /// Вызывается Axum middleware ПОСЛЕ резолва bearer token, ДО того как
//     /// запрос передаётся в DefaultRequestHandler::send_message(), которое
//     /// сгенерирует task_id и вызовет executor.execute(). Требует, чтобы
//     /// task_id был известен ДО execute() — что означает, что либо клиент
//     /// сам присылает task_id (что SDK допускает: req.message.task_id),
//     /// либо нужен другой sync point.
//     pub fn register_caller_for_task(&self, task_id: String, caller: Caller) {
//         self.pending_callers.insert(task_id, caller);
//     }
// }
//
// impl AgentExecutor for AdapterAgentExecutor {
//     fn execute(&self, ctx: ExecutorContext) -> BoxStream<...> {
//         let caller = self.pending_callers
//             .remove(&ctx.task_id)
//             .map(|(_, c)| c)
//             .unwrap_or_else(|| self.default_caller.clone());
//         // ... остальная логика create_and_subscribe использует ЭТОГО caller,
//         // не self.caller
//     }
// }
//
// ПРОБЛЕМА этого плана Б: он требует, чтобы task_id был известен клиенту
// ДО запроса (client-generated task_id), что противоречит типичному A2A
// flow, где task_id часто генерируется сервером (new_task_id() в
// prepare_task_for_execution(), если req.message.task_id пуст). Если
// клиент не присылает task_id, план Б не работает надёжно.

// ============================================================
// ЧАСТЬ 3: единственный надёжный путь — обернуть RequestHandler целиком
// ============================================================
//
// Вывод: план А (переопределить RequestHandler, извлекать caller из
// ServiceParams на входе каждого метода, до вызова DefaultRequestHandler
// или прямого вызова AdapterCore) — единственный надёжный путь, потому что
// params: &ServiceParams доступен в КАЖДОМ методе RequestHandler ДО
// какого-либо task_id-зависимого state, и именно туда Axum middleware
// должен положить resolved Caller.
//
// Чтобы дописать это ТОЧНО, нужно определение:
//   - crate::middleware::ServiceParams (структура, методы, есть ли
//     extensions/headers accessor)
//   - как именно JSON-RPC/REST router (jsonrpc.rs/rest.rs) строит
//     ServiceParams из axum::Request — там, скорее всего, есть текущий
//     механизм передачи headers/auth info, который нужно просто
//     переиспользовать, а не изобретать новый.
//
// Если пришлёте crate::middleware (ServiceParams definition) и/или
// jsonrpc.rs целиком (не фрагмент), я допишу ЭТУ часть окончательно —
// сейчас это последний недостающий кусок для полного завершения
// аутентификации в ACP/HTTP/SSE архитектуре.

// ============================================================
// ЧАСТЬ 4: важное дополнительное наблюдение из прочитанного кода —
// SDK уже поддерживает resume/subscribe с ТОЧНО canonical-совместимой
// защитой от race condition
// ============================================================
//
// subscription_stream() в SDK (строки с SubscriptionState) делает РОВНО то,
// что я рекомендовал в фиксе 3: min_sequence dedup внутри самого SDK:
//
//   loop {
//       match state.receiver.recv().await {
//           Ok(event) if event.sequence <= state.min_sequence => continue, // dedup
//           Ok(event) => { state.min_sequence = event.sequence; ... }
//           Err(RecvError::Lagged(_)) => { /* explicit error, resume needed */ }
//           Err(RecvError::Closed) => return None,
//       }
//   }
//
// Это ПОДТВЕРЖДАЕТ на 100%, что canonical design (history/snapshot first,
// dedup by sequence, explicit Lagged handling) — это то, что сам SDK делает
// внутри ExecutionManager/ActiveExecution. Наш AdapterCore::subscribe()
// должен following ТОТ ЖЕ паттерн — что он и делает после фикса 3. Больше
// никаких изменений в подходе не требуется, диагноз в фиксе 3 окончательно
// подтверждён самим SDK кодом, не только моим рассуждением.
