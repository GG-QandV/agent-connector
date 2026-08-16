#!/usr/bin/env bash
#
# scripts/install.sh — production installer for agent-connector.
#
# Идемпотентен: повторный запуск безопасен (create-if-missing везде).
# Покрывает весь пробел, зафиксированный в ревью:
#   1. cargo build --release
#   2. useradd -r adapterd (без shell, без home)
#   3. mkdir -p /opt/agent-connector/{bin,data}, /etc/agent-connector
#   4. install binary + config + .env skeleton, chown adapterd:adapterd
#   5. install systemd unit из deploy/systemd/, systemctl daemon-reload
#   6. systemctl enable (без --now по умолчанию — явный --start флаг)
#
# Требует root (sudo) для useradd/systemd/chown шагов.
#
# Usage:
#   sudo ./scripts/install.sh [--prefix /opt/agent-connector] [--user adapterd] \
#        [--config path/to/adapter.yaml] [--start] [--skip-build] [--force]
#
# --skip-build   не пересобирать, использовать существующий target/release/adapterd
# --force        перезаписать существующий конфиг/бинарь без запроса подтверждения
# --start        после install сразу systemctl start (по умолчанию только enable)

set -euo pipefail

# ---------- defaults ----------
PREFIX="/opt/agent-connector"
SERVICE_USER="adapterd"
SERVICE_GROUP="adapterd"
CONFIG_SRC=""
SYSTEMD_UNIT_SRC="deploy/systemd/adapterd.service"
ENV_DEST_DEFAULT_NAME=".env"
SKIP_BUILD=0
FORCE=0
START_NOW=0
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------- arg parsing ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --user) SERVICE_USER="$2"; SERVICE_GROUP="$2"; shift 2 ;;
    --config) CONFIG_SRC="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --force) FORCE=1; shift ;;
    --start) START_NOW=1; shift ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

log()  { echo "[install] $*"; }
fail() { echo "[install] ERROR: $*" >&2; exit 1; }

if [[ "$(id -u)" -ne 0 ]]; then
  fail "must be run as root (sudo ./scripts/install.sh ...) — needed for useradd/systemd/chown"
fi

if [[ -z "$CONFIG_SRC" ]]; then
  CONFIG_SRC="$REPO_ROOT/config/adapter.example.yaml"
fi
[[ -f "$CONFIG_SRC" ]] || fail "config source not found: $CONFIG_SRC"

BIN_DEST_DIR="$PREFIX/bin"
DATA_DIR="$PREFIX/data"
CONFIG_DIR="/etc/agent-connector"
CONFIG_DEST="$CONFIG_DIR/adapter.yaml"
ENV_DEST="$CONFIG_DIR/$ENV_DEST_DEFAULT_NAME"
BIN_DEST="$BIN_DEST_DIR/adapterd"

log "prefix=$PREFIX user=$SERVICE_USER config_src=$CONFIG_SRC skip_build=$SKIP_BUILD force=$FORCE start=$START_NOW"

# ---------- 1. build ----------
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  log "building release binary (cargo build --release -p adapterd)"
  ( cd "$REPO_ROOT" && cargo build --release -p adapterd ) \
    || fail "cargo build failed — fix compile errors before installing"
else
  log "skipping build (--skip-build)"
fi

BUILT_BIN="$REPO_ROOT/target/release/adapterd"
[[ -x "$BUILT_BIN" ]] || fail "binary not found at $BUILT_BIN — run without --skip-build first"

# ---------- 2. system user/group ----------
if id "$SERVICE_USER" &>/dev/null; then
  log "user '$SERVICE_USER' already exists, skipping useradd"
else
  log "creating system user '$SERVICE_USER' (no shell, no home)"
  useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER" \
    || fail "useradd failed"
fi

# ---------- 3. directories ----------
log "creating directories: $BIN_DEST_DIR, $DATA_DIR, $CONFIG_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$DATA_DIR"
install -d -o root -g root -m 0755 "$BIN_DEST_DIR"
install -d -o root -g "$SERVICE_GROUP" -m 0750 "$CONFIG_DIR"

# ---------- 4. install binary + config + env skeleton ----------
if [[ -f "$BIN_DEST" && "$FORCE" -eq 0 ]]; then
  log "binary already exists at $BIN_DEST — will overwrite (installer always updates binary)"
fi
install -o root -g root -m 0755 "$BUILT_BIN" "$BIN_DEST"
log "installed binary -> $BIN_DEST"

if [[ -f "$CONFIG_DEST" && "$FORCE" -eq 0 ]]; then
  log "config already exists at $CONFIG_DEST — leaving untouched (use --force to overwrite)"
else
  install -o root -g "$SERVICE_GROUP" -m 0640 "$CONFIG_SRC" "$CONFIG_DEST"
  log "installed config -> $CONFIG_DEST"
fi

if [[ -f "$ENV_DEST" && "$FORCE" -eq 0 ]]; then
  log "env file already exists at $ENV_DEST — leaving untouched (use --force to overwrite)"
else
  cat > "$ENV_DEST" <<'EOF'
# agent-connector runtime environment
# Заполнить перед первым запуском. Ключи ниже — примеры, актуальный
# список смотреть в config/adapter.example.yaml и docs/operations.md.
#
# ADAPTERD_LISTEN=0.0.0.0:8348
# RUST_LOG=info
# DATABASE_URL=postgres://adapterd:CHANGE_ME@localhost/agent_connector
EOF
  chown root:"$SERVICE_GROUP" "$ENV_DEST"
  chmod 0640 "$ENV_DEST"
  log "created env skeleton -> $ENV_DEST (edit before starting service)"
fi

# ---------- 5. systemd unit ----------
UNIT_SRC_PATH="$REPO_ROOT/$SYSTEMD_UNIT_SRC"
[[ -f "$UNIT_SRC_PATH" ]] || fail "systemd unit not found: $UNIT_SRC_PATH"
UNIT_DEST="/etc/systemd/system/adapterd.service"

log "installing systemd unit -> $UNIT_DEST"
# Подстановка PREFIX/User в юнит на случай нестандартных --prefix/--user —
# оригинальный deploy/systemd/adapterd.service жёстко ссылается на
# /opt/agent-connector и User=adapterd; здесь делаем это параметризуемым.
sed \
  -e "s#/opt/agent-connector#${PREFIX}#g" \
  -e "s/^User=.*/User=${SERVICE_USER}/" \
  -e "s/^Group=.*/Group=${SERVICE_GROUP}/" \
  "$UNIT_SRC_PATH" > "$UNIT_DEST"
chmod 0644 "$UNIT_DEST"

log "systemctl daemon-reload"
systemctl daemon-reload

log "systemctl enable adapterd"
systemctl enable adapterd.service

if [[ "$START_NOW" -eq 1 ]]; then
  log "systemctl start adapterd (--start requested)"
  systemctl start adapterd.service
  sleep 1
  systemctl --no-pager status adapterd.service || true
else
  log "service enabled but not started. Review $CONFIG_DEST and $ENV_DEST, then:"
  log "  sudo systemctl start adapterd"
  log "  sudo systemctl status adapterd"
  log "  journalctl -u adapterd -f"
fi

log "install complete."
log "binary:  $BIN_DEST"
log "config:  $CONFIG_DEST"
log "env:     $ENV_DEST"
log "data:    $DATA_DIR (owned by $SERVICE_USER)"
log "unit:    $UNIT_DEST"
