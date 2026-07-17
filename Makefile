MACOSX_DEPLOYMENT_TARGET ?= 14.1
export MACOSX_DEPLOYMENT_TARGET

IOS_DEPLOYMENT_TARGET ?= 17.0
export IPHONEOS_DEPLOYMENT_TARGET := $(IOS_DEPLOYMENT_TARGET)

INFIDELITY_DIR := ../infidelity
HEADERS_DIR := target/xcframework-headers
XCFRAMEWORK := $(INFIDELITY_DIR)/Native/ArchiveWebRTC.xcframework

.PHONY: build build-macos build-ios build-xcframework

build: build-macos

build-macos:
	cargo build --target aarch64-apple-darwin --release
	cargo build --target x86_64-apple-darwin --release
	lipo -create \
		./target/aarch64-apple-darwin/release/libau_tx.a \
		./target/x86_64-apple-darwin/release/libau_tx.a \
		-output $(INFIDELITY_DIR)/autx/Common/libau_tx.a

build-ios:
	cargo build -p archive-webrtc --target aarch64-apple-ios --release
	cargo build -p archive-webrtc --target aarch64-apple-ios-sim --release

build-xcframework: build-macos build-ios
	rm -rf $(HEADERS_DIR) $(XCFRAMEWORK)
	mkdir -p $(HEADERS_DIR) $(INFIDELITY_DIR)/Native
	cp $(INFIDELITY_DIR)/Shared/ArchiveWebRTC.h $(HEADERS_DIR)/ArchiveWebRTC.h
	xcodebuild -create-xcframework \
		-library target/aarch64-apple-ios/release/libarchive_webrtc.a -headers $(HEADERS_DIR) \
		-library target/aarch64-apple-ios-sim/release/libarchive_webrtc.a -headers $(HEADERS_DIR) \
		-output $(XCFRAMEWORK)
