from typing import List
from pathlib import Path

import numpy as np
import pandas as pd

from ab_testing.constants import DISTRIBUTIONS


class FitDistribution:
    def __init__(self, fname: str, data_dir_str: str = "processed_data"):
        self.data_dir_path = Path(data_dir_str)
        self.fname = fname

        data_dir_path = Path(data_dir_str)
        if not data_dir_path.exists():
            data_dir_path.mkdir(parents=True, exist_ok=True)

    def fit(self, data: pd.DataFrame, target: str) -> str:

        df = pd.DataFrame(columns=["distribution", "AIC", "BIC"])

        dists: List[str] = []
        aic: List[float] = []
        bic: List[float] = []
        for dist_name, dist in DISTRIBUTIONS.items():
            params = dist.fit(data[target].values)
            logLik = np.sum(dist.logpdf(data[target].values, *params))
            k, n = len(params), len(data)
            dists.append(dist_name)
            aic.append(2 * k - 2 * logLik)
            bic.append(k * np.log(n) - 2 * logLik)

        df["distribution"] = dists
        df["AIC"] = aic
        df["BIC"] = bic
        df.sort_values(by="AIC", inplace=True)
        df.reset_index(drop=True, inplace=True)

        df.to_parquet(self.data_dir_path / self.fname)

        return df["distribution"].head(1).values[0]
