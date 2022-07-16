import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from feast import FeatureStore
from tests.generators import Generator, iceberg, csv, parquet


def initialize(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend: str):
    feature_store.config.offline_store.backend = backend

    feature_store.config.offline_store.config["spark.sql.extensions"]="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    feature_store.config.offline_store.config["spark.sql.catalog.spark_catalog"]="org.apache.iceberg.spark.SparkSessionCatalog"
    feature_store.config.offline_store.config["spark.sql.catalog.spark_catalog.type"]="hive"
    feature_store.config.offline_store.config["spark.sql.catalog.local"]="org.apache.iceberg.spark.SparkCatalog"
    feature_store.config.offline_store.config["spark.sql.catalog.local.type"]="hadoop"
    feature_store.config.offline_store.config["spark.sql.catalog.local.warehouse"]=tmp_dir


@pytest.mark.parametrize("backend", ["spark"])
@pytest.mark.iceberg
def test_e2e_iceberg_only(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend: str):
    initialize(feature_store, tmp_dir, backend)

    iceberg_fv, iceberg_fv_name = iceberg(tmp_dir)

    entity = Generator.entity()
    feature_store.apply([entity, iceberg_fv])

    entity_df = Generator.entity_df()

    feature_vector = feature_store.get_historical_features(
        features=[
            f"{iceberg_fv_name}:f0"
        ], entity_df=entity_df, full_feature_names=True
    ).to_df()

    print(feature_vector)
    assert(feature_vector[feature_vector.entity_id == 1][f"{iceberg_fv_name}__f0"] is not None)

@pytest.mark.parametrize("backend", ["spark"])
@pytest.mark.iceberg
def test_e2e_iceberg_mix(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend: str):
    initialize(feature_store, tmp_dir, backend)

    iceberg_fv, iceberg_fv_name = iceberg(tmp_dir)
    csv_fv, csv_fv_name = csv(tmp_dir)
    parquet_fv, parquet_fv_name = parquet(tmp_dir)

    entity = Generator.entity()
    feature_store.apply([entity, iceberg_fv, csv_fv, parquet_fv])

    entity_df = Generator.entity_df()

    feature_vector = feature_store.get_historical_features(
        features=[
            f"{iceberg_fv_name}:f0",
            f"{csv_fv_name}:f0",
            f"{parquet_fv_name}:f0",
        ], entity_df=entity_df, full_feature_names=True
    ).to_df()

    assert(feature_vector[feature_vector.entity_id == 1][f"{iceberg_fv_name}__f0"] is not None)
    assert(feature_vector[feature_vector.entity_id == 1][f"{csv_fv_name}__f0"] is not None)
    assert(feature_vector[feature_vector.entity_id == 1][f"{parquet_fv_name}__f0"] is not None)

