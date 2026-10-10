#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "$task_root/run-cloud-e2e.sh" "$@"
