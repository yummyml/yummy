from datetime import datetime
from typing import Callable, Dict, List, Optional, Union

import pandas as pd
import pyarrow
import pyspark.sql.dataframe as sd
import pytz
from delta.tables import DeltaTable
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
from yummy.backends.backend import Backend, BackendType, YummyOfflineStoreConfig

class SparkBackend(Backend):
    def __init__(self, backend_config: BackendConfig):
        super().__init__(backend_config)
        self._spark_session = _get_spark_session(backend_config)

    @property
    def backend_type(self) -> BackendType:
        return BackendType.spark

    @property
    def retrival_job_type(self):
        return SparkRetrievalJob

    @property
    def spark_session(self) -> SparkSession:
        return _spark_session

    @abstractmethod
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

    @abstractmethod
    def get_entity_df_event_timestamp_range(
        self,
        entity_df: Union[pd.DataFrame, Any],
    ) -> Tuple[datetime, datetime]:
        """
        Finds min and max datetime in input entity_df data frame
        """
        ...

    @abstractmethod
    def normalize_timezone(
        self,
        entity_df: Union[pd.DataFrame, Any],
    ) -> Union[pd.DataFrame, Any]:
        """
        Normalize timezon of input entity df to UTC
        """
        ...

    @abstractmethod
    def sort_values(
        self,
        entity_df: Union[pd.DataFrame, Any],
        by: str,
    ) -> Union[pd.DataFrame, Any]:
        """
        Sorts entity df by selected column
        """
        ...

    @abstractmethod
    def field_mapping(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        feature_view: FeatureView,
        features: List[str],
        right_entity_key_columns: List[str],
        entity_df_event_timestamp_col: str,
        event_timestamp_column: str,
        full_feature_names: bool,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def merge(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        df_to_join: Union[pd.DataFrame, Any],
        join_keys: List[str],
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def normalize_timestamp(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        event_timestamp_column: str,
        created_timestamp_column: str,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def filter_ttl(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        feature_view: FeatureView,
        entity_df_event_timestamp_col: str,
        event_timestamp_column: str,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def filter_time_range(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def drop_duplicates(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        all_join_keys: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        entity_df_event_timestamp_col: Optional[str] = None,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def drop_columns(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        event_timestamp_column: str,
        created_timestamp_column: str,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def add_static_column(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        column_name: str,
        column_value: str,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def select(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str]
    ) -> Union[pd.DataFrame, Any]:
        ...

    def _get_spark_session(
        backend_config: YummyOfflineStoreConfig,
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

    def _run_spark_field_mapping(
        self,
        table: sd.DataFrame,
        field_mapping: Dict[str, str],
    ):
        if field_mapping:
            return table.select(
                [col(c).alias(field_mapping.get(c, c)) for c in table.columns]
            )
        else:
            return table


class SparkRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
    ):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = (
            on_demand_feature_views if on_demand_feature_views else []
        )

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
        pass

    def persist(self, storage: SavedDatasetStorage):
        pass


