import os
from .backends.backend import YummyOfflineStore, YummyOfflineStoreConfig, select_all
from .sources.file import ParquetSource, CsvSource
from .sources.delta import DeltaSource
from .sources.iceberg import IcebergSource
from .providers.provider import YummyProvider
from .registries.registry import YummyRegistryStore

class DeprecationHelper(object):
    def __init__(self, new_target, old_name):
        self.new_target = new_target
        self.old_name = old_name

    def _warn(self):
        from warnings import warn
        warn(f"The class {self.old_name} will be replaced with {self.new_target.__name__}")

    def __call__(self, *args, **kwargs):
        self._warn()
        return self.new_target(*args, **kwargs)

    def __getattr__(self, attr):
        self._warn()
        return getattr(self.new_target, attr)

CsvDataSource=DeprecationHelper(CsvSource, "CsvDataSource")
ParquetDataSource=DeprecationHelper(ParquetSource, "ParquetDataSource")
DeltaDataSource=DeprecationHelper(DeltaSource, "DeltaDataSource")
IcebergDataSource=DeprecationHelper(IcebergSource, "IcebergDataSource")

os.environ["FEAST_USAGE"]="False"
__all__ = ["YummyProvider",
           "YummyRegistryStore",
           "YummyOfflineStore",
           "YummyOfflineStoreConfig",
           "ParquetSource",
           "CsvSource",
           "DeltaSource",
           "IcebergSource",
           "ParquetDataSource",
           "CsvDataSource",
           "DeltaDataSource",
           "IcebergDataSource",
           "select_all"]
