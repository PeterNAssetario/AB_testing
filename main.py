import pandas as pd

from ab_testing.constants import client, target_col
from ab_testing.data_acquisition.acquire_data import AcquireBingoALohaData
from ab_testing.distribution_fit.fit_distribution import FitDistribution
from ab_testing.predictions.produce_predictions import ProducePredictions

if client == "bingo_aloha":
    acquire_bingo_aloha_data = AcquireBingoALohaData()
    bingo_aloha_data = acquire_bingo_aloha_data.acquire_data()
else:
    raise NotImplementedError("Given client is not implemented yet.")

fit_dist = FitDistribution()
best_distribution = fit_dist.fit(bingo_aloha_data.loc[bingo_aloha_data[target_col] > 0], target_col)

result = ProducePredictions()
results_conversion = result.produce_results_conversion(bingo_aloha_data)
results_revenue = result.produce_results_revenue(best_distribution, bingo_aloha_data)

output_df = pd.DataFrame(columns=["Metric", "Conversion", "Revenue"])
output_df["Metric"] = ["P( P > C)", "E( loss | P > C)", "E( loss | C > P)"]
output_df["Conversion"] = [
    results_conversion[0]["prob_being_best"],
    results_conversion[0]["expected_loss"],
    results_conversion[1]["expected_loss"],
]
output_df["Revenue"] = [results_revenue[0]["prob_being_best"], results_revenue[0]["expected_loss"], results_revenue[1]["expected_loss"]]

print(f"For clinet {client} data follows {best_distribution} distribution.")
print(output_df)
