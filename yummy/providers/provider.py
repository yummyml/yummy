from feast.infra.local import LocalProvider
from feast.repo_config import RepoConfig
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from yummy import YummyOfflineStoreConfig

class YummyProvider(LocalProvider):

    def __init__(self, config: RepoConfig):
        super().__init__(config)

        if hasattr(config, "backend"):
            config.offline_store.backend=config.backend

        if hasattr(config, "backend_config"):
            config.offline_store.config=config.backend_config



