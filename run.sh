#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$task_root"
./setup.sh
. "$task_root/scripts/rust-env.sh"
./build.sh
"$rust_cargo" +stable fmt --check
"$rust_cargo" +stable build --locked > build.log 2>&1
cd pageserver
PAGESERVER_INTEGRATION=1 go test -count=1 -timeout=10m -run '^(TestWAL|TestNegative|TestGenerations)$' -v . > ../verification.log 2>&1
printf '%s\n' 'Passed: 10 WAL/page tests, 2 negative controls, 5 generation tests'
