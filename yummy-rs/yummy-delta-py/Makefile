MAKE_HELP_LEFT_COLUMN_WIDTH:=14
.PHONY: help build
help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-$(MAKE_HELP_LEFT_COLUMN_WIDTH)s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

fmt: ## cargo fmt
	cargo fmt

clippy: ## cargo clippy --tests
	cargo clippy --tests

dev: ## Develop maturin
	maturin develop --release

build: ## Build the wheel
	rm -rf ../target/wheels
	maturin build --release

publish-testpypi: ## Publish to testpipy
	twine upload --repository testpypi ../target/wheels/*

publish-pypi: ## Publish to pipy
	twine upload --repository pypi ../target/wheels/*

test:
	cargo test -- --show-output
	#cd ../yummy-ml/ && cargo test -- models::lightgbm_model::load_model_and_predict_multiclass  --show-output

