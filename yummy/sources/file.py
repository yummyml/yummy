import json
from datetime import datetime
from typing import Callable, Dict, List, Optional, Union

from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
from yummy.sources.source import YummyDataSource, YummyDataSourceReader
from yummy.backends.backend import BackendType


class YummyFileDataSource(YummyDataSource):
    """Custom data source class for local files"""

    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        path: Optional[str] = None,
        s3_endpoint_override: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        created_timestamp_column: Optional[str] = "",
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
        )
        self._path = path
        self._s3_endpoint_override = s3_endpoint_override

    @abstractmethod
    @property
    def reader_type(self):
        """
        Returns the reader type which will read data source
        """
        ...

    @property
    def path(self):
        """
        Returns the file path of this feature data source.
        """
        return self._path

    @property
    def s3_endpoint_override(self):
        """
        Returns the s3_endpoint_override of this feature data source.
        """
        return self._s3_endpoint_override

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a `CustomFileDataSource` object from a DataSource proto, by
        parsing the CustomSourceOptions which is encoded as a binary json string.
        """
        custom_source_options = str(
            data_source.custom_options.configuration, encoding="utf8"
        )
        path = json.loads(custom_source_options)["path"]
        s3_endpoint_override = json.loads(custom_source_options)["s3_endpoint_override"]
        range_join = json.loads(custom_source_options)["range_join"]
        return DeltaDataSource(
            field_mapping=dict(data_source.field_mapping),
            path=path,
            s3_endpoint_override=s3_endpoint_override,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        """
        Creates a DataSource proto representation of this object, by serializing some
        custom options into the custom_options field as a binary encoded json string.
        """
        config_json = json.dumps(
            {"path": self.path, "s3_endpoint_override": self.s3_endpoint_override, "range_join": self.range_join}
        )
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(config_json, encoding="utf8")
            ),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def get_table_query_string(self) -> str:
        pass

    def validate(self, config: RepoConfig):
        # TODO: validate a FileSource
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.pa_to_feast_value_type


class ParquetDataSource(YummyFileDataSource):
    """Custom data source class for parquets files"""

    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        path: Optional[str] = None,
        s3_endpoint_override: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        created_timestamp_column: Optional[str] = "",
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column=event_timestamp_column,
            path=path,
            s3_endpoint_override=s3_endpoint_override,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
        )

    @abstractmethod
    @property
    def reader_type(self):
        """
        Returns the reader type which will read data source
        """
        return ParquetDataSourceReader


class CsvDataSource(YummyFileDataSource):
    """Custom data source class for parquets files"""

    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        path: Optional[str] = None,
        s3_endpoint_override: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        created_timestamp_column: Optional[str] = "",
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column=event_timestamp_column,
            path=path,
            s3_endpoint_override=s3_endpoint_override,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
        )


class ParquetDataSourceReader(YummyDataSourceReader):

    def read_datasource(
        self,
        data_source,
        features: List[str],
        backend: Backend,
        entity_df: Optional[Union[pd.DataFrame, Any]] = None,
    ) -> Union[pyarrow.Table, pd.DataFrame, Any]:
        backend_type = backend.backend_type
        if backend_type == BackendType.spark:
            from yummy.backends.spark import SparkBackend
            spark_backend: SparkBackend = backend
            spark_session = spark_backend.spark_session
            #TODO: add s3 endpoint override support
            path = data_source.path
            if "s3://" in path:
                path = path.replace('s3://','s3a://')

            return spark_session.read.parquet(path)

        elif backend_type in [BackendType.ray, BackendType.dask]:
            import dask.dataframe as dd
        elif backend_type == BackendType.polars:
            import polars as dd

        return dd.read_parquet(data_source.path, storage_options=self._storage_options(data_source),)

    def _storage_options(self, data_source):
        return (
            {
                "client_kwargs": {
                    "endpoint_url": data_source.file_options.s3_endpoint_override
                }
            }
            if data_source.file_options.s3_endpoint_override
            else None
        )


class CsvDataSourceReader(ParquetDataSourceReader):

    def read_datasource(
        self,
        data_source,
        features: List[str],
        backend: Backend,
        entity_df: Optional[Union[pd.DataFrame, Any]] = None,
    ) -> Union[pyarrow.Table, pd.DataFrame, Any]:
        backend_type = backend.backend_type
        if backend_type == BackendType.spark:
            from yummy.backends.spark import SparkBackend
            spark_backend: SparkBackend = backend
            spark_session = spark_backend.spark_session
            #TODO: add s3 endpoint override support
            path = data_source.path
            if "s3://" in path:
                path = path.replace('s3://','s3a://')

            return spark_session.read.csv(path)

        elif backend_type in [BackendType.ray, BackendType.dask]:
            import dask.dataframe as dd
        elif backend_type == BackendType.polars:
            import polars as dd

        return dd.read_csv(data_source.path, storage_options=self._storage_options(data_source),)

