MAKE_HELP_LEFT_COLUMN_WIDTH:=14
.PHONY: help build
help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-$(MAKE_HELP_LEFT_COLUMN_WIDTH)s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

fmt: ## cargo fmt
	cargo fmt

clippy: ## cargo clippy --tests
	cargo clippy --tests

tt:
	#cargo test -- --show-output
	cd ./yummy-delta/ && cargo test -- apply::test_apply_table  --show-output

tr:
	#cargo test -- --show-output
	cd ./yummy-delta/ && cargo test -- apply::test_read  --show-output

aj:
	#cargo test -- --show-output
	cd ./yummy-delta/ && cargo test -- apply::test_apply_job  --show-output


jj:
	#cargo test -- --show-output
	cd ./yummy-delta/ && cargo test -- delta::jobs::test::test_delta_job_run  --show-output

