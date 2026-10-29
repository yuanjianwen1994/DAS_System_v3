#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VENDOR="$HERE/vendor/linux-toolchain"
RUSTUP_HOME="$VENDOR/rustup-home"
CARGO_HOME="$VENDOR/cargo-home"
mkdir -p "$VENDOR" "$HERE/artifacts"

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  echo "Node.js 22.22.2+ and npm are required. Install them, then rerun this script." >&2
  exit 2
fi
node -e 'const [a,b,c]=process.versions.node.split(".").map(Number); if(a<22 || (a===22 && (b<22 || (b===22 && c<2)))) process.exit(1)' || {
  echo "Node.js 22.22.2 or newer is required." >&2
  exit 2
}

case "$(uname -m)" in
  x86_64) RUSTUP_ARCH=x86_64-unknown-linux-gnu ;;
  aarch64|arm64) RUSTUP_ARCH=aarch64-unknown-linux-gnu ;;
  *) echo "Unsupported Linux architecture: $(uname -m)" >&2; exit 2 ;;
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
cargo build --locked --manifest-path "$HERE/Cargo.toml" --target wasm32-unknown-unknown \
  --release --no-default-features --features stage-contract
cp "$CARGO_TARGET_DIR/wasm32-unknown-unknown/release/das_near_business_flow.wasm" \
  "$HERE/artifacts/stage_contract.wasm"
cargo build --locked --manifest-path "$HERE/Cargo.toml" --target wasm32-unknown-unknown \
  --release --no-default-features --features composite-contract
cp "$CARGO_TARGET_DIR/wasm32-unknown-unknown/release/das_near_business_flow.wasm" \
  "$HERE/artifacts/same_receiver_contract.wasm"

node "$HERE/run_near_dag_experiment.mjs"
