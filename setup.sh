#!/bin/sh
set -eu
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
source_root="$task_root/.deps/turso"
source_commit=8cc059bd951551ae4b9dc31c01146a24b3e87a51
if [ ! -d "$source_root" ]; then
    mkdir -p "$task_root/.deps"
    git clone --filter=blob:none --no-checkout https://github.com/tursodatabase/turso.git "$source_root"
    git -C "$source_root" checkout --detach "$source_commit"
fi
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
