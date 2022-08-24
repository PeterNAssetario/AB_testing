from pathlib import Path

import pandas as pd
from bayesian_testing.experiments import DeltaLognormalDataTest


class ProduceResults:
    def __init__(self, data_dir_str: str = "processed_data", fname: str = "results"):
        self.data_dir_path = Path(data_dir_str)
        self.fname = fname

        data_dir_path = Path(data_dir_str)
        if not data_dir_path.exists():
            data_dir_path.mkdir(parents=True, exist_ok=True)

    def produce(self, distribution: str, df: pd.DataFrame) -> pd.DataFrame:

        if distribution == "lognorm":
            return self._produce_results_lognorm_dist(df)
        else:
            raise NotImplementedError("Results for given distribution are not implemented yet.")

    def _produce_results_lognorm_dist(self, df: pd.DataFrame):

        df_P = df.loc[df["test_group"] == "P"]
        df_C = df.loc[df["test_group"] == "C"]

        assert len(df_P) + len(df_C) == len(df)

        test = DeltaLognormalDataTest()

        test.add_variant_data("P", df_P["total_wins_spend"].values)
        test.add_variant_data("C", df_C["total_wins_spend"].values)

        return test.evaluate(seed=1)
