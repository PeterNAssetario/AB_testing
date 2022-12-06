from pathlib import Path

import pandas as pd

from ab_testing.constants import client_name

from ml_lib.feature_store import configure_offline_feature_store
from ml_lib.feature_store.offline.client import FeatureStoreOfflineClient

from ab_testing.data_acquisition.sql_queries.queries_all_clients import (
    query_homw,
    query_ultimex,
    query_spongebob,
    query_idle_mafia,
    query_terra_genesis,
    query_bingo_aloha,
    query_bingo_aloha_small,
    query_homw_small,
    query_idle_mafia_small,
    query_spongebob_small,
    query_terra_genesis_small,
    query_ultimex_small,
)

configure_offline_feature_store(workgroup="analytics")
# configure_offline_feature_store(workgroup="development", catalog_name="production")

queries_dict = {
    "bingo_aloha": query_bingo_aloha,
    "homw": query_homw,
    "idle_mafia": query_idle_mafia,
    "spongebob": query_spongebob,
    "terra_genesis": query_terra_genesis,
    "ultimex": query_ultimex,
    "bingo_aloha_small": query_bingo_aloha_small,
    "homw_small": query_homw_small,
    "idle_mafia_small": query_idle_mafia_small,
    "spongebob_small": query_spongebob_small,
    "terra_genesis_small": query_terra_genesis_small,
    "ultimex_small": query_ultimex_small,
}


class AcquireData:
    def __init__(self, client: str, fname: str, data_dir_str: str = "raw_data"):
        self.client = client
        self.fname = fname
        self.data_dir_path = Path(data_dir_str)

        if not self.data_dir_path.exists():
            self.data_dir_path.mkdir(parents=True, exist_ok=True)

    def acquire_data(self) -> pd.DataFrame:
        data = self._read_if_exists()
        if data.empty:
            if self.client in queries_dict.keys():
                data = FeatureStoreOfflineClient.run_athena_query_pandas(
                    queries_dict[self.client]
                )
            else:
                raise ValueError(f"Client name {self.client} not found.")
        data.to_parquet(self.data_dir_path / self.fname)

        return data

    def _read_if_exists(self) -> pd.DataFrame:

        if (self.data_dir_path / self.fname).exists():
            df = pd.read_parquet(self.data_dir_path / self.fname)
        else:
            df = pd.DataFrame()

        return df