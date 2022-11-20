import json
from datetime import datetime
from typing import Callable, Dict, List, Optional, Union, Any
import pandas as pd
import pyarrow
from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
from feast.feature_view import FeatureView
from yummy.sources.source import YummyDataSource
from yummy.backends.backend import Backend, BackendType, YummyDataSourceReader

class ConnectorXSource(YummyDataSource):
    """Custom data source class for local files"""

    def __init__(
        self,
        *,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        conn: Optional[str] = None,
        query: Optional[str] = None,
    ):
        super().__init__(
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            description=description,
            tags=tags,
            owner=owner,
            name=name,
            timestamp_field=timestamp_field,
        )
        self._conn = conn
        self._query = query

    @property
    def reader_type(self):
        """
        Returns the reader type which will read data source
        """
        return ConnectorXSourceReader

    @property
    def conn(self):
        """
        Returns the connection of this feature data source.
        """
        return self._conn

    @property
    def query(self):
        """
        Returns the query of this feature data source.
        """
        return self._query


    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a `CustomFileDataSource` object from a DataSource proto, by
        parsing the CustomSourceOptions which is encoded as a binary json string.
        """
        custom_source_options = str(
            data_source.custom_options.configuration, encoding="utf8"
        )
        sql = json.loads(custom_source_options)["sql"]
        return ConnectorXSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
            conn=conn,
            query=query,
        )

    def to_proto(self) -> DataSourceProto:
        """
        Creates a DataSource proto representation of this object, by serializing some
        custom options into the custom_options field as a binary encoded json string.
        """
        config_json = json.dumps(
            {"conn": self.conn, "query": self.query}
        )
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(config_json, encoding="utf8")
            ),
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def get_table_query_string(self) -> str:
        pass

    def validate(self, config: RepoConfig):
        # TODO: validate a DeltaSource
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.pa_to_feast_value_type


class ConnectorXSourceReader(YummyDataSourceReader):

    def read_datasource(
        self,
        data_source,
        features: List[str],
        backend: Backend,
        entity_df: Optional[Union[pd.DataFrame, Any]] = None,
        feature_view: FeatureView = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> Union[pyarrow.Table, pd.DataFrame, Any]:
        backend_type = backend.backend_type
        import connectorx as cx
        conn = data_source.conn
        query = data_source.query
        if backend_type == BackendType.spark:
            from yummy.backends.spark import SparkBackend
            spark_backend: SparkBackend = backend
            spark_session = spark_backend.spark_session

            return spark_session.read.format('delta').load(path)
        elif backend_type in [BackendType.ray, BackendType.dask]:
            return cx.read_sql(conn, query, return_type="dask")
        elif backend_type == BackendType.polars:
            return cx.read_sql(conn, query, return_type="polars")

        raise NotImplementedError(f'Delta lake support not implemented for {backend_type}')

