from .client_config import (
    configure_feature_store_project_scope,
    configure_offline_feature_store,
    configure_online_feature_store,
)
from .offline.client import FeatureStoreOfflineClient
#from .online.client import FeatureStoreOnlineClient
#from .online.datatypes import ProjectDataConfig

__all__ = [
    # Config
    "configure_offline_feature_store",
    "configure_online_feature_store",
    "configure_feature_store_project_scope",
    # Clients
    "FeatureStoreOnlineClient",
    "FeatureStoreOfflineClient",
    # Online data config
    "ProjectDataConfig",
]
