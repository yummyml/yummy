from datetime import datetime
from typing import Callable, Dict, List, Optional, Union

import polars as pl
import pyarrow
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
from yummy.backends.backend import Backend

class PolarsBackend(Backend):
    ...

def _run_polars_field_mapping(
    table: sd.DataFrame,
    field_mapping: Dict[str, str],
):
    if field_mapping:
        return table.rename(field_mapping)
    else:
        return table


class PolarsOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for local (file-based) store"""

    type: Literal["yummy.backends.PolarsOfflineStore"] = "yummy.backends.PolarsOfflineStore"
    """ Offline store type selector"""


class PolarsRetrievalJob(RetrievalJob):
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
        df = self.evaluation_function().to_pandas()
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        return self.evaluation_function().to_arrow()

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        pass

    def persist(self, storage: SavedDatasetStorage):
        pass


class PolarsOfflineStore(OfflineStore):

    spark = None

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:

        if not isinstance(entity_df, pd.DataFrame) and not isinstance(
            entity_df, pl.DataFrame
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )
        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = entity_df.select_dtypes(
                include=["datetime", "datetimetz"]
            ).columns
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

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():

            if isinstance(entity_df, pd.DataFrame):
                # Create a copy of entity_df to prevent modifying the original
                entity_df_with_features = pl.from_pandas(entity_df)
            else:
                entity_df_with_features = entity_df

            # Sort event timestamp values
            entity_df_with_features = entity_df_with_features.orderBy(
                col(entity_df_event_timestamp_col)
            )

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                event_timestamp_column = (
                    feature_view.batch_source.event_timestamp_column
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
                    event_timestamp_column,
                    created_timestamp_column,
                ] + join_keys
                right_entity_key_columns = [c for c in right_entity_key_columns if c]

                # feature_view.batch_source.s3_endpoint_override
                df_to_join = DeltaTable.forPath(
                    spark_session, feature_view.batch_source.path
                ).toDF()

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

                # Ensure that the source dataframe feature column includes the feature view name as a prefix
                df_to_join = _run_spark_field_mapping(df_to_join, columns_map)

                # Select only the columns we need to join from the feature dataframe
                df_to_join = df_to_join.select(
                    [col(c) for c in right_entity_key_columns + feature_names]
                )

                range_join = feature_view.batch_source.range_join
                if range_join:
                    df_to_join = df_to_join.hint("range_join", range_join)

                df_to_join = entity_df_with_features.join(
                    df_to_join, join_keys, "left"
                )

                # Get only data with requested entities
                ttl_seconds = feature_view.ttl.total_seconds()

                if ttl_seconds != 0:
                    df_to_join= df_to_join.filter(
                        (
                            col(event_timestamp_column)
                            >= col(entity_df_event_timestamp_col)
                            - expr(f"INTERVAL {ttl_seconds} seconds")
                        )
                        & (
                            col(event_timestamp_column)
                            <= col(entity_df_event_timestamp_col)
                        )
                    )

                if created_timestamp_column:
                    df_to_join = df_to_join.orderBy(
                        col(created_timestamp_column).desc(),
                        col(event_timestamp_column).desc(),
                    )
                else:
                    df_to_join = df_to_join.orderBy(col(event_timestamp_column).desc())

                df_to_join = df_to_join.dropDuplicates(join_keys)

                # Rename columns by the field mapping dictionary if it exists
                if feature_view.batch_source.field_mapping is not None:
                    df_to_join = _run_spark_field_mapping(
                        df_to_join, feature_view.batch_source.field_mapping
                    )
                # Rename entity columns by the join_key_map dictionary if it exists
                if feature_view.projection.join_key_map:
                    df_to_join = _run_spark_field_mapping(
                        df_to_join, feature_view.projection.join_key_map
                    )

                entity_df_with_features = df_to_join.drop(event_timestamp_column)

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            return entity_df_with_features.persist()

        job = SparkRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
        )
        return job

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:

        spark_session = _get_spark_session(
            config.offline_store
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_offline_job():

            source_df = DeltaTable.forPath(
                    spark_session, data_source.path
                ).toDF()

            source_columns = set(source_df.columns)
            if not set(join_key_columns).issubset(source_columns):
                raise FeastJoinKeysDuringMaterialization(
                    data_source.path, set(join_key_columns), source_columns
                )

            source_df= source_df.filter(
                (col(event_timestamp_column) >= start_date)
                & ( col(event_timestamp_column) <= end_date))

            if created_timestamp_column:
                source_df = source_df.orderBy(
                    col(created_timestamp_column).desc(),
                    col(event_timestamp_column).desc(),
                )
            else:
                source_df = source_df.orderBy(col(event_timestamp_column).desc())

            ts_columns = (
                [event_timestamp_column, created_timestamp_column]
                if created_timestamp_column
                else [event_timestamp_column]
            )

            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )

            if join_key_columns:
                source_df = source_df.dropDuplicates(join_key_columns)
            else:
                source_df.withColumn(DUMMY_ENTITY_ID, lit(DUMMY_ENTITY_VAL))
                columns_to_extract.add(DUMMY_ENTITY_ID)

            return source_df.select([col(c) for c in list(columns_to_extract)])

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return SparkRetrievalJob(
            evaluation_function=evaluate_offline_job,
            full_feature_names=False,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        pass

def _get_spark_session(
    store_config: SparkOfflineStoreConfig,
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()

    if not spark_session:
        spark_builder = SparkSession.builder
        spark_conf = store_config.spark_conf

        if spark_conf:
            spark_builder = spark_builder.config(
                conf=SparkConf().setAll(spark_conf.items())
            )  # noqa

        spark_session = spark_builder.getOrCreate()

    return spark_session
