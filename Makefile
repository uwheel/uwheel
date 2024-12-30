CRATES := $(shell cargo metadata --no-deps --format-version 1 | jq -r '.packages[].manifest_path')

all: build-all test-all

build-all:
	@for crate in $(CRATES); do \
		echo "Building $$crate..."; \
		cargo build --manifest-path $$crate; \
	done

test-all:
	@for crate in $(CRATES); do \
		echo "Testing $$crate..."; \
		cargo test --manifest-path $$crate; \
	done