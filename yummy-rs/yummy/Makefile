MAKE_HELP_LEFT_COLUMN_WIDTH:=14
.PHONY: help build
help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-$(MAKE_HELP_LEFT_COLUMN_WIDTH)s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

build: ## Build the wheel
	rm -r ../target/wheels
	maturin build --release

publish-testpypi: ## Publish to testpipy
	twine upload --repository testpypi ../target/wheels/*

publish-pypi: ## Publish to pipy
	twine upload --repository pypi ../target/wheels/*


test-mlflow:
	cd ../yummy-mlflow/ && cargo test -- --show-output
	#cd ../yummy-mlflow/ && cargo test -- models::lightgbm_model::load_model_and_predict_multiclass  --show-output

test-core:
	cd ../yummy-core/ && cargo test -- --show-output

test-serve:
	cd ../yummy-serve/ && cargo test -- --show-output

