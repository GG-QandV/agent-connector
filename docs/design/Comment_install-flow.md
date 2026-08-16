Готов install-flow, связывающий все компоненты в единый путь: выбор storage → реальная проверка подключения → генерация файлов с правильным разделением секретов → регистрация службы → сводка.

Ключевые решения в этом слое
DSN никогда не попадает в adapter.yaml. Подтвердил через повторную сверку с config-2.rs, что StorageConfig::Postgres несёт только dsn_env: String — имя переменной. resolve_storage() формирует YAML-фрагмент с "dsn-env": DSN_ENV_VAR_NAME, а сам секрет уходит отдельно в env_vars → файл .env. write_config_files() физически пишет их в разные файлы, так что случайный git add adapter.yaml не может утечь пароль.

Реальная проверка подключения, не вера в DSN на слово. validate_postgres_connection() делает настоящий SELECT 1 через tokio_postgres::connect() — и для ExistingPostgres/ExternalManagedPostgres (проверка DSN, который ввёл пользователь), и для ManagedDockerPostgres (проверка, что только что поднятый контейнер реально принимает соединения, уже после wait_for_ready() внутри managed_docker). Это даёт понятную ошибку "SELECT 1 failed: ..." до регистрации systemd/sc.exe службы, а не первый крэш в проде.

Порядок операций минимизирует потери времени при ошибке. Проверка ManagedDockerPostgres без --confirm-docker происходит до cargo build --release — нет смысла тратить минуты на компиляцию, если сам выбор storage невалиден. Это дублирует проверку внутри managed_docker::plan(), но осознанно — быстрый fail-fast важнее чистоты "не повторяться".

Интерактивный prompt только при реальном TTY. resolve_storage_choice() явно проверяет atty::is(Stream::Stdin) — в CI/скриптах без TTY и без --storage флага получаем понятную ошибку NoTty, а не зависание на stdin().read_line() или тихий неверный дефолт.
