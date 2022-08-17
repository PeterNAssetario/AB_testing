from pathlib import Path
from typing import Any, Dict

import pandas as pd
from ml_lib.feature_store import configure_offline_feature_store
from ml_lib.feature_store.offline.client import FeatureStoreOfflineClient

configure_offline_feature_store(workgroup="development", catalog_name="production")


def read_or_query_and_save(
    query: str,
    root_dir: Path,
    fname: str,
    save_to_disk: bool = True,
):

    if (root_dir / fname).exists():
        data = pd.read_parquet(root_dir / fname)
    else:
        data = FeatureStoreOfflineClient.run_athena_query_pandas(query)
        if save_to_disk:
            data.to_parquet(root_dir / fname)

    return data
