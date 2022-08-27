import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from feast import FeatureStore
from tests.generators import Generator, csv, parquet, start_date, end_date
from datetime import datetime, timezone
from yummy import select_all


def entity_df_all():
    return select_all(datetime(2021, 10, 2, 1, 0, 0, tzinfo=timezone.utc))

def e2e_basic(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend: str, entity_df: pd.DataFrame):
    """
    This will test all backends with basic data stores (parquet and csv)
    """
    feature_store.config.offline_store.backend = backend

    csv_fv, csv_fv_name = csv(tmp_dir)
    parquet_fv, parquet_fv_name = parquet(tmp_dir)

    entity = Generator.entity()
    feature_store.apply([entity, csv_fv, parquet_fv])


    feature_vector = feature_store.get_historical_features(
        features=[
            f"{csv_fv_name}:f0",
            f"{parquet_fv_name}:f0",
        ], entity_df=entity_df, full_feature_names=True
    ).to_df()

    feature_store.materialize(start_date=start_date, end_date=end_date)

    fv = feature_vector[feature_vector.entity_id.astype('int') == 1].to_dict(orient="records")[0]
    csv_f0 = float(fv[f"{csv_fv_name}__f0"])
    parquet_f0 = float(fv[f"{parquet_fv_name}__f0"])

    assert(csv_f0 is not None)
    assert(parquet_f0 is not None)

    ofv = feature_store.get_online_features(
        features=[
            f"{csv_fv_name}:f0",
            f"{parquet_fv_name}:f0",
        ],
        entity_rows = [{"entity_id": 1}],
        full_feature_names=True,
    ).to_df().to_dict(orient="records")

    assert(abs(ofv[0]['fv_csv__f0'] - csv_f0) < 1e-6)
    assert(abs(ofv[0]['fv_parquet__f0'] - parquet_f0) < 1e-6)

@pytest.mark.polars
def test_e2e_basic_polars(feature_store: FeatureStore, tmp_dir: TemporaryDirectory):
    """
    This will test polars backend with basic data stores (parquet and csv)
    """
    entity_df = Generator.entity_df()
    e2e_basic(feature_store, tmp_dir, "polars", entity_df)

@pytest.mark.polars
def test_e2e_basic_select_all_polars(feature_store: FeatureStore, tmp_dir: TemporaryDirectory):
    """
    This will test polars backend with basic data stores (parquet and csv)
    """
    e2e_basic(feature_store, tmp_dir, "polars", entity_df_all())


@pytest.mark.dask
def test_e2e_basic_dask(feature_store: FeatureStore, tmp_dir: TemporaryDirectory):
    """
    This will test dask backend with basic data stores (parquet and csv)
    """
    entity_df = Generator.entity_df()
    e2e_basic(feature_store, tmp_dir, "dask", entity_df)

@pytest.mark.dask
def test_e2e_basic_select_all_dask(feature_store: FeatureStore, tmp_dir: TemporaryDirectory):
    """
    This will test dask backend with basic data stores (parquet and csv)
    """
    e2e_basic(feature_store, tmp_dir, "dask", entity_df_all())

@pytest.mark.ray
def test_e2e_basic_ray(feature_store: FeatureStore, tmp_dir: TemporaryDirectory):
    """
    This will test ray backend with basic data stores (parquet and csv)
    """
    entity_df = Generator.entity_df()
    e2e_basic(feature_store, tmp_dir, "ray", entity_df)

@pytest.mark.ray
def test_e2e_basic_select_all_ray(feature_store: FeatureStore, tmp_dir: TemporaryDirectory):
    """
    This will test ray backend with basic data stores (parquet and csv)
    """
    e2e_basic(feature_store, tmp_dir, "ray", entity_df_all())



@pytest.mark.parametrize("backend", ["polars", "dask", "ray"])
@pytest.mark.nospark
def test_e2e_basic_nospark(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend: str):
    """
    This will test all backends with basic data stores (parquet and csv)
    """
    entity_df = Generator.entity_df()
    e2e_basic(feature_store, tmp_dir, backend, entity_df)


@pytest.mark.parametrize("backend", ["spark", "polars", "dask", "ray"])
@pytest.mark.spark
def test_e2e_basic_spark(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend: str):
    """
    This will test all backends with basic data stores (parquet and csv)
    """
    entity_df = Generator.entity_df()
    e2e_basic(feature_store, tmp_dir, backend, entity_df)

@pytest.mark.parametrize("backend", ["spark", "polars", "dask", "ray"])
@pytest.mark.spark
def test_e2e_basic_select_all_spark(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend: str):
    """
    This will test all backends with basic data stores (parquet and csv)
    """
    e2e_basic(feature_store, tmp_dir, backend, entity_df_all())
