build:
	cargo build --target aarch64-apple-darwin --release && cp ./target/aarch64-apple-darwin/release/libau_tx.a ../../infidelity/autx/Common/
