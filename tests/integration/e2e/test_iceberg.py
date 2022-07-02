import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from feast import FeatureStore
from tests.generators import Generator, IcebergGenerator

@pytest.mark.parametrize("backend", ["spark"])
@pytest.mark.spark
def test_end_to_end_one_feature_view(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend):
    feature_store.config.offline_store.backend = backend

    iceberg_dataset = str(Path(tmp_dir) / "local.db.table")
    feature_store.config.offline_store.config["spark.sql.extensions"]="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    feature_store.config.offline_store.config["spark.sql.catalog.spark_catalog"]="org.apache.iceberg.spark.SparkSessionCatalog"
    feature_store.config.offline_store.config["spark.sql.catalog.spark_catalog.type"]="hive"
    feature_store.config.offline_store.config["spark.sql.catalog.local"]="org.apache.iceberg.spark.SparkCatalog"
    feature_store.config.offline_store.config["spark.sql.catalog.local.type"]="hadoop"
    feature_store.config.offline_store.config["spark.sql.catalog.local.warehouse"]=tmp_dir

    iceberg_generator = IcebergGenerator()
    iceberg_fv, iceberg_fv_name = iceberg_generator.generate(iceberg_dataset)

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

