from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union, Dict, Any
from abc import ABC, abstractmethod
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
from feast.importer import import_class
from enum import Enum
import pandas as pd
from datetime import datetime

YUMMY_ALL = "@yummy*"

def select_all(event_timestamp: datetime):
    """
    selects all entities during fetching historical features for specified event timestamp
    """
    return pd.DataFrame.from_dict({YUMMY_ALL: ["*"], DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL: [ event_timestamp ]})


class BackendType(str, Enum):
    dask = "dask"
    ray = "ray"
    spark = "spark"
    polars = "polars"


class BackendConfig(FeastConfigBaseModel):

    backend: Optional[str] = None

    config: Optional[Dict[str, str]] = None
    """ Configuration """


class Backend(ABC):
    """
    Backend implements all operations required to process all offline store steps using
    selected engine
    """
    def __init__(self, backend_config: BackendConfig):
        self._backend_config = backend_config

    @property
    def backend_type(self) -> BackendType:
        raise NotImplementedError("Backend type not defined")

    @property
    def retrival_job_type(self):
        raise NotImplementedError("Retrival job type not defined")

    @abstractmethod
    def first_event_timestamp(
        self,
        entity_df: Union[pd.DataFrame, Any],
        column_name: str
    ) -> datetime:
        """
        Fetch first event timestamp
        """
        ...


    @abstractmethod
    def prepare_entity_df(
        self,
        entity_df: Union[pd.DataFrame, Any],
    ) -> Union[pd.DataFrame, Any]:
        """
        Maps entity_df to type required by backend and finds event timestamp column
        """
        ...

    @abstractmethod
    def normalize_timezone(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        entity_df_event_timestamp_col: str,
    ) -> Union[pd.DataFrame, Any]:
        """
        Normalize timezon of input entity df to UTC
        """
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    def run_field_mapping(
        self,
        table: Union[pd.DataFrame,Any],
        field_mapping: Dict[str, str],
    ):
        ...

    @abstractmethod
    def join(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        df_to_join: Union[pd.DataFrame, Any],
        join_keys: List[str],
        feature_view: FeatureView,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def normalize_timestamp(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        timestamp_field: str,
        created_timestamp_column: str,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def filter_ttl(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        feature_view: FeatureView,
        entity_df_event_timestamp_col: str,
        timestamp_field: str,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def filter_time_range(
        self,
        source_df: Union[pd.DataFrame, Any],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def drop_duplicates(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        subset: List[str],
    ) -> Union[pd.DataFrame, Any]:
        ...

    @abstractmethod
    def drop(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        columns_list: List[str],
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

    def merge(
        self,
        entity_df_with_features: Union[pd.DataFrame, Any],
        df_to_join: Union[pd.DataFrame, Any],
        join_keys: List[str],
        feature_view: FeatureView,
    ) -> Union[pd.DataFrame, Any]:
        # tmp join keys needed for cross join with null join table view
        tmp_join_keys = []
        if not join_keys:
            entity_df_with_features=self.add_static_column(entity_df_with_features, "__tmp", 1)
            df_to_join=self.add_static_column(df_to_join, "__tmp", 1)
            tmp_join_keys = ["__tmp"]

        # Get only data with requested entities
        if YUMMY_ALL in entity_df_with_features.columns:
            df_to_join=self.add_static_column(df_to_join, DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL, self.first_event_timestamp(entity_df_with_features, DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL))
        else:
            df_to_join = self.join(
                entity_df_with_features,
                df_to_join,
                join_keys or tmp_join_keys,
                feature_view,
            )

        if tmp_join_keys:
            df_to_join = self.drop(df_to_join, tmp_join_keys)

        return df_to_join

    def drop_df_duplicates(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        all_join_keys: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        entity_df_event_timestamp_col: Optional[str] = None,
    ) -> Union[pd.DataFrame, Any]:
        if created_timestamp_column:
            df_to_join =  self.sort_values(df_to_join, by=[created_timestamp_column, timestamp_field], na_position="first")
        else:
            df_to_join = self.sort_values(df_to_join, by=timestamp_field, na_position="first")

        if entity_df_event_timestamp_col:
            return self.drop_duplicates(df_to_join, subset=all_join_keys + [entity_df_event_timestamp_col])
        else:
            return self.drop_duplicates(df_to_join, subset=all_join_keys)

    def drop_columns(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        timestamp_field: str,
        created_timestamp_column: str,
    ) -> Union[pd.DataFrame, Any]:
        entity_df_with_features = self.drop(df_to_join, [timestamp_field])

        if created_timestamp_column:
            entity_df_with_features = self.drop(entity_df_with_features,[created_timestamp_column])

        return entity_df_with_features

    def field_mapping(
        self,
        df_to_join: Union[pd.DataFrame, Any],
        feature_view: FeatureView,
        features: List[str],
        right_entity_key_columns: List[str],
        entity_df_event_timestamp_col: str,
        timestamp_field: str,
        full_feature_names: bool,
    ) -> Union[pd.DataFrame, Any]:
        # Rename columns by the field mapping dictionary if it exists
        if feature_view.batch_source.field_mapping:
            df_to_join = self.run_field_mapping(
                df_to_join, feature_view.batch_source.field_mapping
            )
        # Rename entity columns by the join_key_map dictionary if it exists
        if feature_view.projection.join_key_map:
            df_to_join = self.run_field_mapping(
                df_to_join, feature_view.projection.join_key_map
            )

        # Build a list of all the features we should select from this source
        feature_names = []
        columns_map = {}
        for feature in features:
            # Modify the separator for feature refs in column names to double underscore. We are using
            # double underscore as separator for consistency with other databases like BigQuery,
            # where there are very few characters available for use as separators
            if full_feature_names:
                formatted_feature_name = (
                    f"{feature_view.projection.name_to_use()}__{feature}"
                )
            else:
                formatted_feature_name = feature
            # Add the feature name to the list of columns
            feature_names.append(formatted_feature_name)
            columns_map[feature] = formatted_feature_name

        # Ensure that the source dataframe feature column includes the feature view name as a prefix
        df_to_join = self.run_field_mapping(df_to_join, columns_map)

        # Select only the columns we need to join from the feature dataframe
        df_to_join = self.select(df_to_join, right_entity_key_columns + feature_names)

        # Make sure to not have duplicated columns
        if entity_df_event_timestamp_col == timestamp_field:
            df_to_join = self.run_field_mapping(
                df_to_join, {timestamp_field: f"__{timestamp_field}"},
            )
            timestamp_field = f"__{timestamp_field}"

        return df_to_join, timestamp_field

    def get_entity_df_event_timestamp_range(
        self,
        entity_df: Union[pd.DataFrame, Any],
        entity_df_event_timestamp_col: str,
    ) -> Tuple[datetime, datetime]:
        """
        Finds min and max datetime in input entity_df data frame
        """
        if isinstance(entity_df, pd.DataFrame):
            entity_df_event_timestamp = entity_df.loc[entity_df_event_timestamp_col].infer_objects()
            if pd.api.types.is_string_dtype(entity_df_event_timestamp):
                entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)

            return (
                entity_df_event_timestamp.min().to_pydatetime(),
                entity_df_event_timestamp.max().to_pydatetime(),
            )

        # TODO: check if this metadata is really needed
        return None, None

    def columns_list(
        self,
        entity_df: Union[pd.DataFrame, Any],
    ) -> List[str]:
        """
        Reads columns list
        """
        return entity_df.columns

    def select_dtypes_columns(
        self,
        entity_df: Union[pd.DataFrame, Any],
        include: List[str]
    ) -> List[str]:
        return entity_df.select_dtypes(include=include).columns

    def create_retrival_job(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ) -> RetrievalJob:
        return self.retrival_job_type(
            evaluation_function=evaluation_function,
            full_feature_names=full_feature_names,
            on_demand_feature_views=on_demand_feature_views,
            metadata=metadata,
        )

    def read_datasource(
        self,
        data_source,
        features: List[str],
        entity_df: Optional[Union[pd.DataFrame, Any]] = None,
    ) -> Union[pyarrow.Table, pd.DataFrame, Any]:
        """
        Reads data source
        """
        assert issubclass(data_source.reader_type, YummyDataSourceReader)
        reader: YummyDataSourceReader = data_source.reader_type()
        return reader.read_datasource(data_source, features, self, entity_df)


class BackendFactory:

    @staticmethod
    def create(
        backend_type: BackendType,
        backend_config: BackendConfig) -> Backend:

        print(f'I will use {backend_type} backend')

        if backend_type == BackendType.dask:
            from yummy.backends.dask import DaskBackend
            return DaskBackend(backend_config)
        elif backend_type == BackendType.ray:
            import ray
            import dask
            from ray.util.dask import ray_dask_get
            ray.init(ignore_reinit_error=True)
            dask.config.set(scheduler=ray_dask_get)
            from yummy.backends.dask import DaskBackend
            return DaskBackend(backend_config)
        elif backend_type == BackendType.spark:
            from yummy.backends.spark import SparkBackend
            return SparkBackend(backend_config)
        elif backend_type == BackendType.polars:
            from yummy.backends.polars import PolarsBackend
            return PolarsBackend(backend_config)

        return PolarsBackend(backend_config)


class YummyDataSourceReader(ABC):

    @abstractmethod
    def read_datasource(
        self,
        data_source,
        features: List[str],
        backend: Backend,
        entity_df: Optional[Union[pd.DataFrame, Any]] = None,
    ) -> Union[pyarrow.Table, pd.DataFrame, Any]:
        ...


class YummyOfflineStoreConfig(BackendConfig):
    """Offline store config for local (file-based) store"""

    type: Literal["yummy.YummyOfflineStore"] = "yummy.YummyOfflineStore"
    """ Offline store type selector"""


class YummyOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="yummy")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:

        backend_type = config.offline_store.backend
        backend = BackendFactory.create(backend_type, config.offline_store)
        entity_df = backend.prepare_entity_df(entity_df)
        all_columns = backend.columns_list(entity_df)

        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in all_columns:
            datetime_columns = backend.select_dtypes_columns(
                entity_df,
                include=["datetime", "datetimetz"]
            )
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )

        (
            feature_views_to_features,
            on_demand_feature_views_to_features,
        ) = _get_requested_feature_views_to_features_dict(
            feature_refs,
            feature_views,
            registry.list_on_demand_feature_views(config.project),
        )

        entity_df_event_timestamp_range = backend.get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():

            entity_df_with_features = backend.normalize_timezone(entity_df, entity_df_event_timestamp_col)

            entity_df_with_features = backend.sort_values(entity_df_with_features, entity_df_event_timestamp_col)

            join_keys = []
            all_join_keys = []

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                timestamp_field = (
                    feature_view.batch_source.timestamp_field
                )
                created_timestamp_column = (
                    feature_view.batch_source.created_timestamp_column
                )

                # Build a list of entity columns to join on (from the right table)
                join_keys = []

                for entity_name in feature_view.entities:
                    entity = registry.get_entity(entity_name, project)
                    join_key = feature_view.projection.join_key_map.get(
                        entity.join_key, entity.join_key
                    )
                    join_keys.append(join_key)

                right_entity_key_columns = [
                    timestamp_field,
                    created_timestamp_column,
                ] + join_keys
                right_entity_key_columns = [c for c in right_entity_key_columns if c]

                all_join_keys = list(set(all_join_keys + join_keys))

                df_to_join = backend.read_datasource(feature_view.batch_source, features, entity_df_with_features)

                df_to_join, timestamp_field = backend.field_mapping(
                    df_to_join,
                    feature_view,
                    features,
                    right_entity_key_columns,
                    entity_df_event_timestamp_col,
                    timestamp_field,
                    full_feature_names,
                )

                df_to_join = backend.merge(entity_df_with_features, df_to_join, join_keys, feature_view)

                df_to_join = backend.normalize_timestamp(
                    df_to_join, timestamp_field, created_timestamp_column
                )


                df_to_join = backend.filter_ttl(
                    df_to_join,
                    feature_view,
                    entity_df_event_timestamp_col,
                    timestamp_field,
                )

                df_to_join = backend.drop_df_duplicates(
                    df_to_join,
                    all_join_keys,
                    timestamp_field,
                    created_timestamp_column,
                    entity_df_event_timestamp_col,
                )

                entity_df_with_features = backend.drop_columns(
                    df_to_join, timestamp_field, created_timestamp_column
                )

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            return entity_df_with_features

        job = backend.create_retrival_job(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(all_columns) - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )
        return job

    @staticmethod
    @log_exceptions_and_usage(offline_store="yummy")
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:

        backend_type = config.offline_store.backend
        backend = BackendFactory.create(backend_type, config.offline_store)

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_offline_job():
            source_df = backend.read_datasource(data_source, feature_name_columns)

            source_df = backend.normalize_timestamp(
                source_df, timestamp_field, created_timestamp_column
            )

            all_columns = backend.columns_list(source_df)

            source_columns = set(all_columns)
            if not set(join_key_columns).issubset(source_columns):
                raise FeastJoinKeysDuringMaterialization(
                    data_source.path, set(join_key_columns), source_columns
                )

            ts_columns = (
                [timestamp_field, created_timestamp_column]
                if created_timestamp_column
                else [timestamp_field]
            )

            source_df = backend.filter_time_range(source_df, timestamp_field, start_date, end_date)

            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )
            if join_key_columns:
                source_df = backend.drop_df_duplicates(source_df, join_key_columns, timestamp_field, created_timestamp_column)
            else:
                source_df = backend.add_static_column(source_df, DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL)
                columns_to_extract.add(DUMMY_ENTITY_ID)


            return backend.select(source_df, list(columns_to_extract))

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return backend.create_retrival_job(
            evaluation_function=evaluate_offline_job, full_feature_names=False,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        return YummyOfflineStore.pull_latest_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns
            + [timestamp_field],  # avoid deduplication
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=None,
            start_date=start_date,
            end_date=end_date,
        )


