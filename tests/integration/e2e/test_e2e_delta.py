import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from feast import FeatureStore
from tests.generators import Generator, csv, parquet, delta


@pytest.mark.parametrize("backend", ["polars", "dask", "ray", "spark"])
@pytest.mark.delta
def test_e2e_delta(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend):
    """
    This will test all backends with basic data stores and delta lake store
    """
    feature_store.config.offline_store.backend = backend

    csv_fv, csv_fv_name = csv(tmp_dir)
    parquet_fv, parquet_fv_name = parquet(tmp_dir)
    delta_fv, delta_fv_name = delta(tmp_dir)

    entity = Generator.entity()
    feature_store.apply([entity, csv_fv, parquet_fv, delta_fv])

    entity_df = Generator.entity_df()

    feature_vector = feature_store.get_historical_features(
        features=[
            f"{csv_fv_name}:f0",
            f"{parquet_fv_name}:f0",
            f"{delta_fv_name}:f0"
        ], entity_df=entity_df, full_feature_names=True
    ).to_df()

    assert(feature_vector[feature_vector.entity_id == 1][f"{csv_fv_name}__f0"] is not None)
    assert(feature_vector[feature_vector.entity_id == 1][f"{parquet_fv_name}__f0"] is not None)
    assert(feature_vector[feature_vector.entity_id == 1][f"{delta_fv_name}__f0"] is not None)

