import json
from datetime import datetime
from typing import Callable, Dict, List, Optional, Union

from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
from yummy.sources.source import YummyDataSource

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

