from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union, Dict

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
from yummy.backends.backend import Backend, BackendType



def _run_dask_field_mapping(
    table: dd.DataFrame, field_mapping: Dict[str, str],
):
    if field_mapping:
        # run field mapping in the forward direction
        table = table.rename(columns=field_mapping)
        table = table.persist()

    return table

class DaskBackend(Backend):
    def __init__(self, backend_config: BackendConfig):
        super().__init__(backend_config)

    @property
    def backend_type(self) -> BackendType:
        return BackendType.dask

    @property
    def retrival_job_type(self):
        return DaskRetrievalJob

    @abstractmethod
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
            entity_df: dd.DataFrame = dd.from_pandas(entity_df)

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


