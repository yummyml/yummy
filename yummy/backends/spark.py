from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union, Dict, Any

import pandas as pd
import pyarrow
import pyspark.sql.dataframe as sd
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
from pyspark.sql.functions import col, expr, lit
from pyspark.sql import SparkSession
from pyspark import SparkConf
from yummy.backends.backend import Backend, BackendType, BackendConfig

class SparkBackend(Backend):
    def __init__(self, backend_config: BackendConfig):
        super().__init__(backend_config)
        self._spark_session = self._get_spark_session(backend_config)

    @property
    def backend_type(self) -> BackendType:
        return BackendType.spark

    @property
    def retrival_job_type(self):
        return SparkRetrievalJob

    @property
    def spark_session(self) -> SparkSession:
        return self._spark_session

    def first_event_timestamp(
        self,
        entity_df: Union[pd.DataFrame, Any],
        column_name: str
    ) -> datetime:
        """
        Fetch first event timestamp
        """
        return entity_df.toPandas()[column_name][0]

    def prepare_entity_df(
        self,
        entity_df: Union[pd.DataFrame, Any],
    ) -> Union[pd.DataFrame, Any]:
        """
        Maps entity_df to type required by backend and finds event timestamp column
        """
        if not isinstance(entity_df, pd.DataFrame) and not isinstance(
            entity_df, sd.DataFrame
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )

        if isinstance(entity_df, pd.DataFrame):
            entity_df: sd.DataFrame = self.spark_session.createDataFrame(entity_df)

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

        if type(by) is str:
            by = [by]

        cols = [col(b) for b in by]

        if ascending and na_position=='first':
            cols = [col(b).asc_nulls_first() for b in by]
        elif ascending and na_position=='last':
            cols = [col(b).asc_nulls_last() for b in by]
        elif not ascending and na_position=='first':
            cols = [col(b).desc_nulls_first() for b in by]
        elif not ascending and na_position=='last':
            cols = [col(b).desc_nulls_last() for b in by]

        return entity_df.orderBy(cols)

    def run_field_mapping(
        self,
        table: Union[pd.DataFrame,Any],
        field_mapping: Dict[str, str],
    ):
        if field_mapping:
            return table.select(
                [col(c).alias(field_mapping.get(c, c)) for c in table.columns]
            )
        else:
            return table

    def join(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        df_to_join: Union[pd.DataFrame, Any],
        join_keys: List[str],
        feature_view: FeatureView,
    ) -> Union[pd.DataFrame, Any]:
        range_join = None
        if hasattr(feature_view.batch_source, 'range_join'):
            range_join = feature_view.batch_source.range_join

        if range_join:
            df_to_join = df_to_join.hint("range_join", range_join)

        df_to_join_cols = df_to_join.columns
        entity_df_with_features_columns = [c for c in entity_df_with_features.columns if c not in df_to_join_cols]
        cols = df_to_join_cols+entity_df_with_features_columns

        return entity_df_with_features.join(
            other=df_to_join,
            on=join_keys,
            how="left"
        ).select(*cols)

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
            ttl_seconds = feature_view.ttl.total_seconds()
            df_to_join= df_to_join.filter(
                (
                    col(timestamp_field)
                    >= col(entity_df_event_timestamp_col)
                    - expr(f"INTERVAL {ttl_seconds} seconds")
                )
                & (
                    col(timestamp_field)
                    <= col(entity_df_event_timestamp_col)
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
            (col(timestamp_field) >= start_date)
            & (col(timestamp_field) < end_date)
        )

    def drop_duplicates(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        subset: List[str],
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.dropDuplicates(subset)

    def drop(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str],
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.drop(*columns_list)

    def add_static_column(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        column_name: str,
        column_value: str,
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.withColumn(column_name, lit(column_value))

    def select(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str]
    ) -> Union[pd.DataFrame, Any]:
        return df_to_join.select(*columns_list)

    def drop_df_duplicates(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        all_join_keys: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        entity_df_event_timestamp_col: Optional[str] = None,
    ) -> Union[pd.DataFrame, Any]:
        # This must be overriten and df must be reversed because in pyspark
        # there is no keep last in dropDuplicates
        if created_timestamp_column:
            df_to_join =  self.sort_values(df_to_join, ascending=False, by=[created_timestamp_column, timestamp_field], na_position="last")
        else:
            df_to_join = self.sort_values(df_to_join, ascending=False, by=timestamp_field, na_position="last")

        if entity_df_event_timestamp_col:
            return self.drop_duplicates(df_to_join,subset=all_join_keys + [entity_df_event_timestamp_col])
        else:
            return self.drop_duplicates(df_to_join,subset=all_join_keys)

    def _get_spark_session(
        self,
        backend_config: BackendConfig,
    ) -> SparkSession:
        spark_session = SparkSession.getActiveSession()

        if not spark_session:
            spark_builder = SparkSession.builder
            spark_conf = backend_config.config

            if spark_conf:
                spark_builder = spark_builder.config(
                    conf=SparkConf().setAll(spark_conf.items())
                )  # noqa

            spark_session = spark_builder.getOrCreate()

        return spark_session


class SparkRetrievalJob(RetrievalJob):
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
        df = self.evaluation_function().toPandas()
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().toPandas()
        return pyarrow.Table.from_pandas(df)

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        self._metadata

    def persist(self, storage: SavedDatasetStorage):
        pass


