#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VENDOR="$HERE/vendor"
RUSTUP_HOME="$VENDOR/macos-toolchain/rustup-home"
CARGO_HOME="$VENDOR/macos-toolchain/cargo-home"
mkdir -p "$VENDOR/macos-toolchain"

case "$(uname -m)" in
  arm64|aarch64) RUSTUP_ARCH=aarch64-apple-darwin ;;
  x86_64) RUSTUP_ARCH=x86_64-apple-darwin ;;
  *) echo "Unsupported macOS architecture: $(uname -m)" >&2; exit 2 ;;
esac

if [[ ! -x "$CARGO_HOME/bin/cargo" ]]; then
  RUSTUP_INIT="$VENDOR/rustup-init-$RUSTUP_ARCH"
  curl --fail --location --silent --show-error \
    "https://static.rust-lang.org/rustup/dist/$RUSTUP_ARCH/rustup-init" \
    --output "$RUSTUP_INIT"
  chmod +x "$RUSTUP_INIT"
  RUSTUP_HOME="$RUSTUP_HOME" CARGO_HOME="$CARGO_HOME" \
    "$RUSTUP_INIT" -y --no-modify-path --profile minimal \
    --default-toolchain 1.99.1 --target wasm32-unknown-unknown
fi

export RUSTUP_HOME CARGO_HOME
export RUSTUP_TOOLCHAIN=1.99.1
export PATH="$CARGO_HOME/bin:$PATH"
rustup target add wasm32-unknown-unknown --toolchain 1.99.1

PNPM="$VENDOR/npm-tools/node_modules/.bin/pnpm"
if [[ ! -x "$PNPM" ]]; then
  npm install --prefix "$VENDOR/npm-tools" --no-audit --no-fund pnpm@10.18.3
fi
"$PNPM" --dir "$HERE" install --frozen-lockfile --ignore-scripts

export CARGO_TARGET_DIR="$HERE/target"
cargo build --locked --manifest-path "$HERE/Cargo.toml" \
  --target wasm32-unknown-unknown --release --no-default-features \
  --features stage-contract
cp "$CARGO_TARGET_DIR/wasm32-unknown-unknown/release/das_near_business_flow.wasm" \
  "$HERE/artifacts/stage_contract.wasm"
cargo build --locked --manifest-path "$HERE/Cargo.toml" \
  --target wasm32-unknown-unknown --release --no-default-features \
  --features composite-contract
cp "$CARGO_TARGET_DIR/wasm32-unknown-unknown/release/das_near_business_flow.wasm" \
  "$HERE/artifacts/same_receiver_contract.wasm"

if [[ -d "$VENDOR/xz-prefix/lib" ]]; then
  export DYLD_LIBRARY_PATH="$VENDOR/xz-prefix/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
elif command -v brew >/dev/null 2>&1 && [[ -d "$(brew --prefix xz)/lib" ]]; then
  export DYLD_LIBRARY_PATH="$(brew --prefix xz)/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi

node "$HERE/run_near_dag_experiment.mjs"
