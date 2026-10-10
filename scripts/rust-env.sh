# Sourced by the runners after task_root is set. Prefer an existing local
# toolchain for this workspace; fresh clones use the installed rustup/cargo.
if [ -x "$task_root/.cargo/bin/cargo" ] && [ -d "$task_root/.rustup/toolchains" ]; then
    export RUSTUP_HOME="$task_root/.rustup"
    export CARGO_HOME="$task_root/.cargo"
    rust_cargo="$CARGO_HOME/bin/cargo"
else
    rust_cargo=cargo
fi
