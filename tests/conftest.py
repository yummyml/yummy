import os
import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from _pytest.tmpdir import TempPathFactory
from feast import FeatureStore
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.repo_config import load_repo_config, RepoConfig
from filelock import FileLock
from datetime import datetime, timedelta
#from example_feature_repo.example import generate_example_data, example_data_exists


@pytest.fixture(scope="session")
def root_path() -> str:
    """Fixture that returns the root path of this git repository"""
    return str((Path(__file__).parent / "..").resolve())

@pytest.fixture(scope="session")
def example_repo_path(root_path: str) -> str:
    """Fixture that returns the path of the example feature repository"""
    return str(Path(root_path) / "example_feature_repo")


@pytest.fixture(scope="function")
def tmp_dir(root_path: str) -> TemporaryDirectory:
    """Fixture that returns a temp dir, which is cleaned up automatically after tests"""
    with TemporaryDirectory(dir=root_path) as tmp_dir:
        yield tmp_dir


@pytest.fixture(scope="function")
def repo_config(tmp_dir: TemporaryDirectory, example_repo_path: str) -> RepoConfig:
    """
    Fixture that returns a repo config to be used for testing
    Config is based on example_feature_repo/feature_store.yaml, but with two important
    changes:
     - registry store (sqllite) points to a tmp dir
     - online store (sqllite) points to the same tmp dir
    If we don't do this, all tests will run agains the same registry/online store
    instances, which is not possible with parallel test execution.
    """
    repo_config = load_repo_config(repo_path=Path(example_repo_path))
    repo_config.registry = str(Path(tmp_dir) / "registry.db")
    repo_config.online_store.path = str(Path(tmp_dir) / "online_store.db")
    return repo_config

@pytest.fixture(scope="function")
def feature_store(repo_config: RepoConfig, example_repo_path: str) -> FeatureStore:
    """Fixture that returns a feature store instance that can be used in parallel tests"""
    feature_store = FeatureStore(config=repo_config)
    feature_store.repo_path = str(example_repo_path)
    return feature_store

