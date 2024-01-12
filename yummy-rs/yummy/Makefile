MAKE_HELP_LEFT_COLUMN_WIDTH:=14
.PHONY: help build
help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-$(MAKE_HELP_LEFT_COLUMN_WIDTH)s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

build: ## Build the wheel
	rm -rf ../target/wheels
	maturin build --release

publish-testpypi: ## Publish to testpipy
	twine upload --repository testpypi ../target/wheels/*

publish-pypi: ## Publish to pipy
	twine upload --repository pypi ../target/wheels/*


test-ml:
	cd ../yummy-ml/ && cargo test -- --show-output
	#cd ../yummy-ml/ && cargo test -- models::lightgbm_model::load_model_and_predict_multiclass  --show-output

test-core:
	cd ../yummy-core/ && cargo test -- --show-output

test-features:
	cd ../yummy-features/ && cargo test -- --show-output

test-delta:
	cd ../yummy-delta/ && cargo test -- --show-output

run-ml-serve:
	cargo run --features yummy-ml ml serve --model /home/jovyan/yummy/yummy-rs/tests/mlflow/lightgbm_model/lightgbm_my_model/ --port 8080 --host 0.0.0.0 --loglevel debug

run-delta-serve:
	cargo run --features yummy-delta delta serve --config /home/jovyan/yummy/yummy-rs/tests/delta/apply.yaml --port 8080 --host 0.0.0.0 --loglevel debug


