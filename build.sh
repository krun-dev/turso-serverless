#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
mkdir -p "$task_root/bin"
mkdir -p "$task_root/evidence"
cd "$task_root/pageserver"
go build -trimpath -o ../bin/pageserver ./cmd/pageserver
go build -trimpath -o ../bin/verify ./cmd/verify
