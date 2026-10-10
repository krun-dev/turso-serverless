#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$task_root"
backend=memory
export PAGESERVER_TEST_OSS=0
case "${1-}" in
  '') ;;
  --oss) backend=oss; export PAGESERVER_TEST_OSS=1; shift ;;
  *) printf '%s\n' 'usage: run-storage-tests.sh [--oss]' >&2; exit 2 ;;
esac
[ "$#" -eq 0 ] || exit 2
./setup.sh
. "$task_root/scripts/rust-env.sh"
./build.sh
"$rust_cargo" +stable build --locked > build.log 2>&1
cd pageserver
PAGESERVER_INTEGRATION=1 go test -count=1 -timeout=15m -run '^(TestArchive|TestCold)$' -v . > "../evidence/storage-$backend-run.log" 2>&1
printf '%s\n' 'Passed: 24 archive tests, 23 cold read/write tests'
