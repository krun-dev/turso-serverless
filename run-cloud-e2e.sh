#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$task_root"
. "$task_root/scripts/rust-env.sh"
unset E2E_FILTER E2E_STRICT E2E_SKIPPED_DIAGNOSTICS E2E_SKIPPED_PERSISTENT
backend=memory
case "${1-}" in
  '') ;;
  --oss) backend=oss ;;
  *) printf '%s\n' 'usage: run-cloud-e2e.sh [--oss]' >&2; exit 2 ;;
esac
[ "$#" -le 1 ] || exit 2
./run.sh
./run-storage-tests.sh "$@"
"$rust_cargo" +stable build --locked --profile corpus --features official-e2e --bin official-e2e
./bin/verify official "$@"
./bin/verify official "$@" --strict
# Only the exact two upstream in-memory VACUUM limitations may fail.
# Missing output or any added failure is rejected by the final coverage check.
if [ -e "evidence/skipped-cloud-$backend-latest.json" ]; then
    rm -- "evidence/skipped-cloud-$backend-latest.json"
fi
./bin/verify official "$@" --skipped-diagnostics sqlite-sqltests/gencol.sqltest || true
./bin/verify official "$@" --skipped-diagnostics --persistent-diagnostics sqlite-sqltests/gencol.sqltest
./bin/verify summary "$@"
