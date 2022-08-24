from abc import ABC
from pathlib import Path

import pandas as pd
from ab_testing.data_acquisition.data_acquisition_utils import read_or_query_and_save
from ab_testing.data_acquisition.sql_queries.bingo_aloha_queries import (
    query_bingo_aloha,
    query_bingo_aloha_30,
)


class BaseDataAcquisition(ABC):
    def __init__(self, data_dir_str: str, fname: str):
        self.data_dir_path = Path(data_dir_str)
        self.fname = fname

        data_dir_path = Path(data_dir_str)
        if not data_dir_path.exists():
            data_dir_path.mkdir(parents=True, exist_ok=True)

    def _read_if_exists(self) -> pd.DataFrame:

        if (self.data_dir_path / self.fname).exists():
            df = pd.read_parquet(self.data_dir_path / self.fname)
        else:
            df = pd.DataFrame()

        return df


class AcquireBingoALohaData(BaseDataAcquisition):
    def __init__(self, data_dir_str: str = "raw_data", fname: str = "bingo_aloha_data.p"):
        super().__init__(
            data_dir_str=data_dir_str,
            fname=fname,
        )

    def acquire_data(self) -> pd.DataFrame:

        bingo_aloha_data = self._read_if_exists()

        if bingo_aloha_data.empty:

            bingo_aloha_data = read_or_query_and_save(
                query_bingo_aloha,
                self.data_dir_path,
                "bingo_aloha_data.p",
            )

        bingo_aloha_data.to_parquet(self.data_dir_path / self.fname)

        return bingo_aloha_data
