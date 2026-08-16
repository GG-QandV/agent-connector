## Готова полная реализация Windows-слоя

покрывает и десктопные 10/11, и Server, с явными различиями там, где они реально есть.

Ключевые архитектурные решения для Windows
Служба через нативный SCM, не сторонний wrapper. Регистрация через sc.exe create с obj="NT SERVICE\adapterd" — виртуальный service account создаётся автоматически системой, никакого эквивалента useradd не требуется вообще. Добавил sc.exe failure с restart-политикой (restart/3000 × 3 раза за 24ч) — это прямой аналог Restart=on-failure/RestartSec=3 из вашего реального adapterd.service.

Явная проверка на admin-права до любых действий. require_admin() через net session — если icacls/sc.exe запускаются без прав, они часто тихо ничего не делают или дают невнятную ошибку; лучше зафейлиться сразу с понятным сообщением "запустите из elevated PowerShell", чем оставить систему в частично настроенном состоянии.

ACL через icacls, зеркалящий ProtectSystem=full + ReadWritePaths. grant_data_dir_access() даёt write только data/ каталогу (SYSTEM + service account + Administrators), а lock_down_read_only() — конфигу/бинарю только (RX) для сервисного аккаунта. Это прямой перенос вашей реальной security-модели из systemd unit, не более слабая Windows-версия.

Различие Windows 10/11 vs Server, которое я не мог придумать заранее
Docker connectivity — только named pipe, никогда TCP fallback. И десктопный Docker Desktop, и нативный Docker Engine на Server слушают на npipe:////./pipe/docker_engine при стандартной установке. TCP-endpoint почти всегда означает daemon, открытый без TLS — installer сознательно не пытается TCP автоматически, только пишет явную ошибку, если named pipe недоступен, вместо тихой деградации на менее безопасный путь.

Проверка Linux containers mode перед managed-docker-postgres. Это специфично для Windows: Docker может быть в "Windows containers" режиме (актуально для Server с нативными Windows контейнерами), а Postgres-образ — Linux-based. assert_linux_containers_mode() явно проверяет docker info и даёт понятную инструкцию переключения вместо непонятного image pull failure.

Что проверено юнит-тестом сразу
docker_endpoint_is_named_pipe_not_tcp — зафиксировал инвариант "никогда TCP по умолчанию" как исполняемый тест, не только комментарий, потому что это тот тип решения, где будущий рефакторинг мог бы случайно "оптимизировать" в сторону TCP без понимания последствий для безопасности.
