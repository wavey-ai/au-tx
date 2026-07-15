MACOSX_DEPLOYMENT_TARGET ?= 14.1
export MACOSX_DEPLOYMENT_TARGET

build:
	cargo build --target aarch64-apple-darwin --release
	cargo build --target x86_64-apple-darwin --release
	lipo -create \
		./target/aarch64-apple-darwin/release/libau_tx.a \
		./target/x86_64-apple-darwin/release/libau_tx.a \
		-output ../infidelity/autx/Common/libau_tx.a
