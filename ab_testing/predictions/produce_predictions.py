from typing import Dict, List, Union

import numpy as np
import pandas as pd

# https://github.com/Matt52/bayesian-testing
from bayesian_testing.experiments import BinaryDataTest, DeltaLognormalDataTest

from ab_testing.constants import target_col


class ProducePredictions:
    def __init__(self, seed: int = 42):

        self.seed = seed

    def produce_results_revenue(
        self, dist_name: str, df: pd.DataFrame
    ) -> List[Dict[str, Union[float, int]]]:

        if dist_name == "lognorm":
            return self._produce_results_lognorm_dist(df)
        else:
            raise NotImplementedError(
                "Results for given distribution are not implemented yet."
            )

    def produce_results_conversion(
        self, df: pd.DataFrame
    ) -> List[Dict[str, Union[float, int]]]:
        dfc = df.copy()

        dfc["conversions"] = np.where(dfc[target_col] > 0, 1, 0)

        df_P = dfc.loc[
            (dfc["test_group"].str.lower() == "p")
            | (dfc["test_group"].str.lower() == "assetario")
        ]
        df_C = dfc.loc[
            (dfc["test_group"].str.lower() == "c")
            | (dfc["test_group"].str.lower() == "control")
        ]

        test = BinaryDataTest()

        test.add_variant_data("P", df_P["conversions"].values)
        test.add_variant_data("C", df_C["conversions"].values)

        return test.evaluate(seed=self.seed)

    def _produce_results_lognorm_dist(
        self, df: pd.DataFrame
    ) -> List[Dict[str, Union[float, int]]]:

        df_P = df.loc[
            (df["test_group"].str.lower() == "p")
            | (df["test_group"].str.lower() == "assetario")
        ]
        df_C = df.loc[
            (df["test_group"].str.lower() == "c")
            | (df["test_group"].str.lower() == "control")
        ]

        test = DeltaLognormalDataTest()

        test.add_variant_data("P", df_P["total_wins_spend"].values)
        test.add_variant_data("C", df_C["total_wins_spend"].values)
        
        return test.evaluate(seed=42)

    def _produce_results_lognorm_dist_carry_value(self, df: pd.DataFrame) -> list:
        df_P = df.loc[
            (df["test_group"].str.lower() == "p")
            | (df["test_group"].str.lower() == "assetario")
        ]
        df_C = df.loc[
            (df["test_group"].str.lower() == "c")
            | (df["test_group"].str.lower() == "control")
        ]

        test = DeltaLognormalDataTest()

        test.add_variant_data("P", df_P["total_wins_spend"].values)
        test.add_variant_data("C", df_C["total_wins_spend"].values)

        return test.carry_value(seed=42)
