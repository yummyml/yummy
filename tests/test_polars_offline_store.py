import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from feast import FeatureStore
from tests.generators import Generator, CsvGenerator, ParquetGenerator, DeltaGenerator

@pytest.mark.parametrize("backend", ["polars", "dask", "ray", "spark"])
def test_end_to_end_one_feature_view(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend):
    feature_store.config.offline_store.backend = backend

    csv_dataset = str(Path(tmp_dir) / "data.csv")
    csv_generator = CsvGenerator()
    csv_fv, csv_fv_name = csv_generator.generate(csv_dataset)

    parquet_dataset = str(Path(tmp_dir) / "data.parquet")
    parquet_generator = ParquetGenerator()
    parquet_fv, parquet_fv_name = parquet_generator.generate(parquet_dataset)

    delta_dataset = str(Path(tmp_dir) / "delta")
    delta_generator = DeltaGenerator()
    delta_fv, delta_fv_name = delta_generator.generate(delta_dataset)


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

