from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union, Dict, Any

import dask.dataframe as dd
import pandas as pd
import pyarrow
import pytz
from pydantic.typing import Literal

from feast import FileSource, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import FeastJoinKeysDuringMaterialization
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.provider import (
    _get_requested_feature_views_to_features_dict,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage
from yummy.backends.backend import Backend, BackendType, BackendConfig


class DaskBackend(Backend):
    def __init__(self, backend_config: BackendConfig):
        super().__init__(backend_config)

    @property
    def backend_type(self) -> BackendType:
        return BackendType.dask

    @property
    def retrival_job_type(self):
        return DaskRetrievalJob

    def prepare_entity_df(
        self,
        entity_df: Union[pd.DataFrame, Any],
    ) -> Union[pd.DataFrame, Any]:
        """
        Maps entity_df to type required by backend and finds event timestamp column
        """
        if not isinstance(entity_df, pd.DataFrame) and not isinstance(
            entity_df, dd.DataFrame
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )

        if isinstance(entity_df, pd.DataFrame):
            entity_df: dd.DataFrame = dd.from_pandas(entity_df, npartitions=1)

        return entity_df

    def first_event_timestamp(
        self,
        entity_df: Union[pd.DataFrame, Any],
        column_name: str
    ) -> datetime:
        """
        Fetch first event timestamp
        """
        return entity_df.compute()[column_name][0]


    def normalize_timezone(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        entity_df_event_timestamp_col: str,
    ) -> Union[pd.DataFrame, Any]:
        """
        Normalize timezon of input entity df to UTC
        """
        entity_df_event_timestamp_col_type = entity_df_with_features.dtypes[
            entity_df_event_timestamp_col
        ]
        if (
            not hasattr(entity_df_event_timestamp_col_type, "tz")
            or entity_df_event_timestamp_col_type.tz != pytz.UTC
        ):
            # Make sure all event timestamp fields are tz-aware. We default tz-naive fields to UTC
            entity_df_with_features[
                entity_df_event_timestamp_col
            ] = entity_df_with_features[entity_df_event_timestamp_col].apply(
                lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
                meta=(entity_df_event_timestamp_col, "datetime64[ns, UTC]"),
            )

            # Convert event timestamp column to datetime and normalize time zone to UTC
            # This is necessary to avoid issues with pd.merge_asof
            if isinstance(entity_df_with_features, dd.DataFrame):
                entity_df_with_features[
                    entity_df_event_timestamp_col
                ] = dd.to_datetime(
                    entity_df_with_features[entity_df_event_timestamp_col], utc=True
                )
            else:
                entity_df_with_features[
                    entity_df_event_timestamp_col
                ] = pd.to_datetime(
                    entity_df_with_features[entity_df_event_timestamp_col], utc=True
                )

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
        return entity_df.sort_values(by, ascending=ascending, na_position=na_position).persist()

    def run_field_mapping(
        self,
        table: Union[pd.DataFrame,Any],
        field_mapping: Dict[str, str],
    ):
        if field_mapping:
            # run field mapping in the forward direction
            table = table.rename(columns=field_mapping)
            table = table.persist()

        return table

    def join(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        df_to_join: Union[pd.DataFrame, Any],
        join_keys: List[str],
        feature_view: FeatureView,
    ) -> Union[pd.DataFrame, Any]:
        return dd.merge(
                entity_df_with_features,
                df_to_join,
                left_on=join_keys,
                right_on=join_keys,
                suffixes=("", "__"),
                how="left",
            )

    def normalize_timestamp(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        timestamp_field: str,
        created_timestamp_column: str,
    ) -> Union[pd.DataFrame, Any]:
        df_to_join_types = df_to_join.dtypes
        timestamp_field_type = df_to_join_types[timestamp_field]

        if created_timestamp_column:
            created_timestamp_column_type = df_to_join_types[created_timestamp_column]

        if (
            not hasattr(timestamp_field_type, "tz")
            or timestamp_field_type.tz != pytz.UTC
        ):
            # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
            df_to_join[timestamp_field] = df_to_join[timestamp_field].apply(
                lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
                meta=(timestamp_field, "datetime64[ns, UTC]"),
            )

        if created_timestamp_column and (
            not hasattr(created_timestamp_column_type, "tz")
            or created_timestamp_column_type.tz != pytz.UTC
        ):
            df_to_join[created_timestamp_column] = df_to_join[
                created_timestamp_column
            ].apply(
                lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
                meta=(timestamp_field, "datetime64[ns, UTC]"),
            )

        return df_to_join.persist()

    def filter_ttl(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        feature_view: FeatureView,
        entity_df_event_timestamp_col: str,
        timestamp_field: str,
    ) -> Union[pd.DataFrame, Any]:
        # Filter rows by defined timestamp tolerance
        if feature_view.ttl and feature_view.ttl.total_seconds() != 0:
            df_to_join = df_to_join[
                (
                    df_to_join[timestamp_field]
                    >= df_to_join[entity_df_event_timestamp_col] - feature_view.ttl
                )
                & (
                    df_to_join[timestamp_field]
                    <= df_to_join[entity_df_event_timestamp_col]
                )
            ]

            df_to_join = df_to_join.persist()

        return df_to_join

    def filter_time_range(
        self,
        source_df: Union[pd.DataFrame, Any],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Union[pd.DataFrame, Any]:
        source_df = source_df[
            (source_df[timestamp_field] >= start_date)
            & (source_df[timestamp_field] < end_date)
        ]

        return source_df.persist()

    def drop_duplicates(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        subset: List[str],
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.drop_duplicates(subset, keep='last', ignore_index=True,).persist()

    def drop(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str],
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.drop(columns_list, axis=1).persist()

    def add_static_column(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        column_name: str,
        column_value: str,
    ) -> Union[pd.DataFrame, Any]:
        df_to_join[column_name]=column_value
        return df_to_join

    def select(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str]
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join[columns_list].persist()

class DaskRetrievalJob(RetrievalJob):
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
        df = self.evaluation_function().compute()
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        return pyarrow.Table.from_pandas(df)

    def persist(self, storage: SavedDatasetStorage):
        assert isinstance(storage, SavedDatasetFileStorage)

        filesystem, path = FileSource.create_filesystem_and_path(
            storage.file_options.file_url, storage.file_options.s3_endpoint_override,
        )

        if path.endswith(".parquet"):
            pyarrow.parquet.write_table(
                self.to_arrow(), where=path, filesystem=filesystem
            )
        else:
            # otherwise assume destination is directory
            pyarrow.parquet.write_to_dataset(
                self.to_arrow(), root_path=path, filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata


