from contextlib import contextmanager
from typing import Dict, Generator, Iterable, List, Optional, Type, Union
from unittest.mock import patch

import pandas as pd

from .offline.client import FeatureStoreOfflineClient
from .online.client import FeatureStoreOnlineClient, TRow
from .online.datatypes import ContextDataConfig, ProjectDataConfig


@contextmanager
def patch_query_context_data(
    config: Dict[str, pd.DataFrame]
) -> Generator[None, None, None]:
    def mock__query_context_data(data_config: ContextDataConfig) -> pd.DataFrame:
        return config[data_config.name]

    with patch.object(
        target=FeatureStoreOnlineClient,
        attribute="query_context_data",
        new=mock__query_context_data,
    ):
        yield


@contextmanager
def patch_query_project_data(
    config: Dict[str, pd.DataFrame]
) -> Generator[None, None, None]:
    def mock__query_project_data(data_config: ProjectDataConfig) -> pd.DataFrame:
        return config[data_config.name]

    with patch.object(
        target=FeatureStoreOnlineClient,
        attribute="query_project_data",
        new=mock__query_project_data,
    ):
        yield


@contextmanager
def patch_query_project_data_row(
    config: Dict[str, TRow]
) -> Generator[None, None, None]:
    def mock__query_project_data_row(
        data_config: ProjectDataConfig,
        row_cls: Type[TRow],
    ) -> TRow:
        return config[data_config.name]

    with patch.object(
        target=FeatureStoreOnlineClient,
        attribute="query_project_data_row",
        new=mock__query_project_data_row,
    ):
        yield


@contextmanager
def patch_query_project_data_row_v2(
    config: Dict[str, Optional[TRow]]
) -> Generator[None, None, None]:
    def mock__query_project_data_row_v2(
        data_config: ProjectDataConfig,
        row_cls: Type[TRow],
    ) -> Optional[TRow]:
        return config[data_config.name]

    with patch.object(
        target=FeatureStoreOnlineClient,
        attribute="query_project_data_row_v2",
        new=mock__query_project_data_row_v2,
    ):
        yield


class MissingMockReturnValueError(Exception):
    pass


class FeatureStoreOfflineClientMocker:
    def __init__(self, return_values: List[pd.DataFrame] = []):
        self.return_values = return_values

    def yield_value(
        self,
        sql: str,
        parameters: Dict[str, Union[str, float, int]] = dict(),
        na_values: Optional[Iterable[str]] = ("",),
    ) -> pd.DataFrame:

        if len(self.return_values) == 0:
            raise MissingMockReturnValueError(
                "FeatureStoreOfflineClientMocker called but return_values are empty. Init with more data or call append_responses."
            )

        return self.return_values.pop(0)

    def append_responses(self, *responses: pd.DataFrame) -> None:
        for resp in responses:
            self.return_values.append(resp)


@contextmanager
def patch_run_athena_query_pandas(
    config: List[pd.DataFrame],
) -> Generator[FeatureStoreOfflineClientMocker, None, None]:

    mocker = FeatureStoreOfflineClientMocker(config)
    with patch.object(
        target=FeatureStoreOfflineClient,
        attribute="run_athena_query_pandas",
        new=mocker.yield_value,
    ):
        yield mocker
