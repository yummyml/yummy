from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union, Dict, Any

import polars as pl
import pandas as pd
import pyarrow
import pytz
from feast import FileSource, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import FeastJoinKeysDuringMaterialization
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.provider import _get_requested_feature_views_to_features_dict
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage
from pydantic.typing import Literal
from yummy.backends.backend import Backend, BackendType, BackendConfig

class PolarsBackend(Backend):
    def __init__(self, backend_config: BackendConfig):
        super().__init__(backend_config)

    @property
    def backend_type(self) -> BackendType:
        return BackendType.polars

    @property
    def retrival_job_type(self):
        return PolarsRetrievalJob

    def first_event_timestamp(
        self,
        entity_df: Union[pd.DataFrame, Any],
        column_name: str
    ) -> datetime:
        """
        Fetch first event timestamp
        """
        return entity_df[column_name][0]


    def prepare_entity_df(
        self,
        entity_df: Union[pd.DataFrame, Any],
    ) -> Union[pd.DataFrame, Any]:
        """
        Maps entity_df to type required by backend and finds event timestamp column
        """
        if not isinstance(entity_df, pd.DataFrame) and not isinstance(
            entity_df, pl.DataFrame
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )

        if isinstance(entity_df, pd.DataFrame):
            entity_df: pl.DataFrame = pl.from_pandas(entity_df)

        return entity_df

    def normalize_timezone(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        entity_df_event_timestamp_col: str,
    ) -> Union[pd.DataFrame, Any]:
        """
        Normalize timezon of input entity df to UTC
        """
        return entity_df_with_features

    def sort_values(
        self,
        entity_df: Union[pd.DataFrame, Any],
        by: Union[str,List[str]],
        ascending: bool = True,
        na_position: Optional[str] = "last",
    ) -> Union[pd.DataFrame, Any]:
        """
        Sorts entity df by selected column
        """
        nulls_last = True
        if na_position == 'first':
            nulls_last = False

        reverse = False
        if not ascending:
            reverse = True

        return entity_df.sort(by, reverse=reverse, nulls_last=nulls_last)

    def run_field_mapping(
        self,
        table: Union[pd.DataFrame,Any],
        field_mapping: Dict[str, str],
    ):
        if field_mapping:
            # run field mapping in the forward direction
            table = table.rename(field_mapping)

        return table

    def join(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        df_to_join: Union[pd.DataFrame, Any],
        join_keys: List[str],
        feature_view: FeatureView,
    ) -> Union[pd.DataFrame, Any]:
        return entity_df_with_features.join(
                df_to_join,
                left_on=join_keys,
                right_on=join_keys,
                suffix="__",
                how="left",
            )

    def normalize_timestamp(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        timestamp_field: str,
        created_timestamp_column: str,
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join

    def filter_ttl(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        feature_view: FeatureView,
        entity_df_event_timestamp_col: str,
        timestamp_field: str,
    ) -> Union[pd.DataFrame, Any]:
        # Filter rows by defined timestamp tolerance
        if feature_view.ttl and feature_view.ttl.total_seconds() != 0:
            df_to_join = df_to_join.filter(
                (
                    pl.col(timestamp_field)
                    >= pl.col(entity_df_event_timestamp_col) - feature_view.ttl
                )
                & (
                    pl.col(timestamp_field)
                    <= pl.col(entity_df_event_timestamp_col)
                )
            )

        return df_to_join

    def filter_time_range(
        self,
        source_df: Union[pd.DataFrame, Any],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Union[pd.DataFrame, Any]:
        return source_df.filter(
            (pl.col(timestamp_field) >= start_date)
            & (pl.col(timestamp_field) < end_date)
        )

    def drop_duplicates(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        subset: List[str],
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.unique(subset=subset, keep='last')

    def drop(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str],
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.drop(columns_list)

    def add_static_column(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        column_name: str,
        column_value: str,
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.with_column(pl.lit(column_value).alias(column_name))

    def select(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str]
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.select([pl.col(c) for c in columns_list])


class PolarsRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = (
            on_demand_feature_views if on_demand_feature_views else []
        )
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    @log_exceptions_and_usage
    def _to_df_internal(self) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().to_pandas()
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        return self.evaluation_function().to_arrow()

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        self._metadata

    def persist(self, storage: SavedDatasetStorage):
        pass

