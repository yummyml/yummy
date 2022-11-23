import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
from feast import FeatureStore
from tests.generators import Generator, connectorx


@pytest.mark.parametrize("backend", ["polars"])
@pytest.mark.connectorx
def test_e2e_connectorx(feature_store: FeatureStore, tmp_dir: TemporaryDirectory, backend):
    """
    This will test all backends with basic data stores and delta lake store
    """
    feature_store.config.offline_store.backend = backend

    connectorx_fv, connectorx_fv_name = connectorx(tmp_dir)

    entity = Generator.entity()
    feature_store.apply([entity, connectorx_fv])

    entity_df = Generator.entity_df()

    feature_vector = feature_store.get_historical_features(
        features=[
            f"{connectorx_fv_name}:f0",
        ], entity_df=entity_df, full_feature_names=True
    ).to_df()

    assert(feature_vector[feature_vector.entity_id == 1][f"{connectorx_fv_name}__f0"] is not None)

