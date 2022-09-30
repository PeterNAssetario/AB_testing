from typing import Dict, List, Union

import pandas as pd


def generate_output_dataframe(
    *,
    results_conversion: List[Dict[str, Union[float, int]]],
    results_revenue: List[Dict[str, Union[float, int]]],
) -> pd.DataFrame:

    output_df = pd.DataFrame(columns=["Metric", "Conversion", "Revenue"])
    output_df["Metric"] = ["P( P > C)", "E( loss | P > C)", "E( loss | C > P)"]
    output_df["Conversion"] = [
        results_conversion[0]["prob_being_best"],
        results_conversion[0]["expected_loss"],
        results_conversion[1]["expected_loss"],
    ]
    output_df["Revenue"] = [
        results_revenue[0]["prob_being_best"],
        results_revenue[0]["expected_loss"],
        results_revenue[1]["expected_loss"],
    ]

    return output_df
