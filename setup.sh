#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
source_root="$task_root/.deps/turso"
source_commit=$(git -C "$task_root" ls-files --stage -- .deps/turso | awk '$1 == "160000" { print $2 }')
if [ -z "$source_commit" ]; then
    printf '%s\n' 'Missing Turso submodule pin; clone this repository with Git.' >&2
    exit 1
fi
git -C "$task_root" submodule update --init --recursive -- .deps/turso
if [ "$(git -C "$source_root" rev-parse HEAD)" != "$source_commit" ]; then
    printf '%s\n' 'Turso source revision differs from the pinned commit; use a fresh .deps/turso checkout.' >&2
    exit 1
fi
if [ -n "$(git -C "$source_root" status --porcelain --untracked-files=no)" ]; then
    printf '%s\n' 'Pinned Turso source has local modifications.' >&2
    exit 1
fi
mkdir -p "$task_root/evidence"
printf '%s\n' "Turso source ready: $source_commit"
