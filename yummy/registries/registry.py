import uuid
import os
from datetime import datetime
from pathlib import Path

from feast.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.usage import log_exceptions_and_usage

class YummyRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        self._registry: RegistryStore = self._factory(registry_config, repo_path)

    def _factory(self, registry_config: RegistryConfig, repo_path: Path):
        path = registry_config.path

        if path.startswith("s3://"):
            from feast.infra.aws import S3RegistryStore
            if hasattr(registry_config, "s3_endpoint_override"):
                os.environ["FEAST_S3_ENDPOINT_URL"]=registry_config.s3_endpoint_override
            return S3RegistryStore(registry_config, repo_path)
        elif path.startswith("gs://"):
            from feast.infra.gcp import GCSRegistryStore
            return GCSRegistryStore(registry_config, repo_path)
        else:
            from feast.infra.local import LocalRegistryStore
            return LocalRegistryStore(registry_config, repo_path)

    @log_exceptions_and_usage(registry="local")
    def get_registry_proto(self):
        return self._registry.get_registry_proto()

    @log_exceptions_and_usage(registry="local")
    def update_registry_proto(self, registry_proto: RegistryProto):
        self._registry.update_registry_proto(registry_proto)

    def teardown(self):
        self._registry.teardown()

    def _write_registry(self, registry_proto: RegistryProto):
        self._registry._write_registry(registry_proto)

