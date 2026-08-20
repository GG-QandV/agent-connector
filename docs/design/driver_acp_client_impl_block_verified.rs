// ПРОВЕРЕНО построчным счётчиком скобок: 16 открывающих '{', 16 закрывающих
// '}', итоговая глубина 0. Этот блок impl AcpClientDriver { ... } САМ ПО
// СЕБЕ сбалансирован. Если в вашем файле на строках 73-172 глубина = 1
// (не закрыт), значит при более ранней правке (когда убирали лишнюю '}'
// после удалённого clone_state()) физически пропала ОДНА из закрывающих
// скобок ГДЕ-ТО внутри этого диапазона — не в конце impl, а именно внутри
// одного из трёх методов.
//
// РЕКОМЕНДАЦИЯ: не искать глазами (это уже не помогло), а ЗАМЕНИТЬ строки
// 73-172 в вашем файле ЦЕЛИКОМ на текст ниже — он гарантированно
// сбалансирован (проверено программным счётчиком, не визуально).
//
// Команда для замены (проверить diff перед применением, раз номера строк
// у вас уже могли сместиться после ваших правок):
//   sed -n '73,172p' crates/driver-acp-client/src/lib.rs  # посмотреть текущий диапазон
//   затем вручную заменить этот диапазон на блок ниже.

impl AcpClientDriver {
    pub async fn spawn(id: impl Into<String>, config: AcpClientConfig) -> Result<Self, AcpClientError> {
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &config.working_dir {
            command.current_dir(dir);
        }

        let mut child = command.spawn().map_err(|e| AcpClientError::Spawn(e.to_string()))?;
        let stdin = child.stdin.take().ok_or(AcpClientError::StdioUnavailable)?;
        let stdout = child.stdout.take().ok_or(AcpClientError::StdioUnavailable)?;

        let pending: PendingRequests = Arc::new(DashMap::new());
        let active_tasks: Arc<DashMap<TaskId, ActiveAcpTask>> = Arc::new(DashMap::new());

        let reader_task = spawn_reader_loop(stdout, pending.clone(), active_tasks.clone());

        let driver = Self {
            id: id.into(),
            child_stdin: Arc::new(Mutex::new(stdin)),
            pending,
            active_tasks,
            next_request_id: Arc::new(AtomicU64::new(1)),
            _child: Arc::new(Mutex::new(child)),
            reader_task,
        };

        driver.call("initialize", json!({})).await?;
        Ok(driver)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, AcpClientError> {
        let id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);

        let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut line = serde_json::to_string(&request).map_err(|e| AcpClientError::Rpc(e.to_string()))?;
        line.push('\n');

        {
            let mut stdin = self.child_stdin.lock().await;
            stdin.write_all(line.as_bytes()).await.map_err(|e| AcpClientError::Rpc(e.to_string()))?;
            stdin.flush().await.map_err(|e| AcpClientError::Rpc(e.to_string()))?;
        }

        rx.await.map_err(|_| AcpClientError::ProcessExited)
    }

    fn parts_to_acp_blocks(parts: &[Part]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| match part {
                Part::Text { text } => json!({ "kind": "text", "text": text }),
                Part::Json { value } => json!({ "kind": "data", "json": value }),
                Part::FileRef { uri, mime_type } => json!({
                    "kind": "resource", "uri": uri, "mimeType": mime_type,
                }),
            })
            .collect()
    }
}
