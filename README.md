# elvcrawl
eluvio fabric crawler

Install Rust and add nightly toolchain and wasm32 targets
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
rustup toolchain install nightly
rustup update
rustup target add wasm32-unknown-unknown --toolchain nightly
rustup default nightly

Building
Rust
cargo build --target wasm32-unknown-unknown --release --workspace

