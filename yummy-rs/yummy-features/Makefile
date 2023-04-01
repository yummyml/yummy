MAKE_HELP_LEFT_COLUMN_WIDTH:=14
.PHONY: help build
help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-$(MAKE_HELP_LEFT_COLUMN_WIDTH)s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

fmt: ## cargo fmt
	cargo fmt

clippy: ## cargo clippy --tests
	cargo clippy --tests

build: ## Build the wheel
	cargo build --release

