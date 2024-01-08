import json
from datetime import datetime
from typing import Callable, Dict, List, Optional, Union, Iterable, Tuple, Any
from abc import ABC, abstractmethod
from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType
import pyarrow
import pandas as pd

class YummyDataSource(DataSource):
    """Custom data source class for local files"""

    def __init__(
        self,
        *,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
    ):
        super().__init__(
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            description=description,
            tags=tags,
            owner=owner,
            name=name,
            timestamp_field=timestamp_field,
        )

    @property
    def reader_type(self):
        """
        Returns the reader type which will read data source
        """
        raise NotImplementedError("Reader type not defined")

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        """
        Creates a `CustomFileDataSource` object from a DataSource proto, by
        parsing the CustomSourceOptions which is encoded as a binary json string.
        """
        raise NotImplementedError

    def to_proto(self) -> DataSourceProto:
        """
        Creates a DataSource proto representation of this object, by serializing some
        custom options into the custom_options field as a binary encoded json string.
        """
        raise NotImplementedError

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return _map_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns the list of column names and raw column types.
        Args:
            config: Configuration object used to configure a feature store.
        """
        return self.reader_type().read_schema(self, config)

    def get_table_query_string(self) -> str:
        """
        Returns a string that can directly be used to reference this table in SQL.
        """
        raise NotImplementedError



source_type_map = {
    "int32": ValueType.INT32,
    "int64": ValueType.INT64,
    "double": ValueType.DOUBLE,
    "float": ValueType.FLOAT,
    "float64": ValueType.DOUBLE,
    "string": ValueType.STRING,
    "binary": ValueType.BYTES,
    "bool": ValueType.BOOL,
    "object": ValueType.UNKNOWN,
    "utf8": ValueType.STRING,
    "null": ValueType.NULL,
}

def _map_value_type(pa_type_as_str: str) -> ValueType:
    is_list = False
    if pa_type_as_str.startswith("list<item: "):
        is_list = True
        pa_type_as_str = pa_type_as_str.replace("list<item: ", "").replace(">", "")

    if pa_type_as_str.startswith("timestamp"):
        value_type = ValueType.UNIX_TIMESTAMP
    elif pa_type_as_str.startswith("datetime"):
        value_type = ValueType.UNIX_TIMESTAMP
    else:
        value_type = source_type_map[pa_type_as_str]

    if is_list:
        value_type = ValueType[value_type.name + "_LIST"]

    return value_type
