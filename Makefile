MAKE_HELP_LEFT_COLUMN_WIDTH:=14
.PHONY: help build
help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-$(MAKE_HELP_LEFT_COLUMN_WIDTH)s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

format: ## Format all the code using isort and black
	isort feast_pyspark/
	black --target-version py37 feast_pyspark

lint: ## Run mypy, isort, flake8, and black
	mypy feast_pyspark/
	isort feast_pyspark/ --check-only
	flake8 feast_pyspark/
	black --check feast_pyspark

build: ## Build the wheel
	rm -rf dist/*
	python setup.py sdist
#	python -m build

publish-testpypi: ## Publish to testpipy
	twine upload --repository testpypi dist/*

publish-pypi: ## Publish to pipy
	twine upload --repository pypi dist/*

# Here we have to type out the whole command for the test rather than having
# `cd feast && FULL_REPO_CONFIGS_MODULE=tests.repo_config make test-python-universal`
# The reason is that feast runs the tests in parallel and doing so the update function
# is run in parallel where two threads will try to create the same schema at the same
# time.
#
# https://stackoverflow.com/a/29908840/957738
#   If the schema is being concurrently created in another session but isn't yet committed,
#   then it both exists and does not exist, depending on who you are and how you look.
#   It's not possible for other transactions to "see" the new schema in the system
#   catalogs because it's uncommitted, so it's entry in pg_namespace is not visible to
#   other transactions. So CREATE SCHEMA / CREATE TABLE tries to create it because, as
#   far as it's concerned, the object doesn't exist.
# 
test-python-universal:
	cd feast && FULL_REPO_CONFIGS_MODULE=tests.repo_config FEAST_USAGE=False IS_TEST=True python -m pytest --integration --universal sdk/python/tests
