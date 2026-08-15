#!/usr/bin/env bash
# Локальный запуск adapterd с SQLite profile.
# Первый аргумент — путь к конфигу (дефолт adapter.yaml).
set -euo pipefail
cd "$(dirname "$0")/.."

CONFIG="${1:-adapter.yaml}"
if [ ! -f "$CONFIG" ]; then
  echo "Конфиг не найден: $CONFIG" >&2
  echo "Скопируйте образец: cp config/adapter.example.yaml adapter.yaml" >&2
  exit 1
fi

export RUST_LOG="${RUST_LOG:-info}"
exec cargo run -p adapterd -- "$CONFIG"
