from ab_testing.constants import target_col, client_name
from ab_testing.general_utils import generate_output_dataframe
from ab_testing.data_acquisition.acquire_data import AcquireData
from ab_testing.predictions.produce_predictions import ProducePredictions
from ab_testing.distribution_fit.fit_distribution import FitDistribution

acquire_initial_data = AcquireData(client=client_name, fname=f"{client_name}_data.p")
initial_data = acquire_initial_data.acquire_data()


fit_dist = FitDistribution(fname=f"{client_name}_distribution_fit.p")
best_distribution = fit_dist.fit(
    initial_data.loc[initial_data[target_col] > 0], target_col
)

result = ProducePredictions()
results_conversion = result.produce_results_conversion(initial_data)
results_revenue = result.produce_results_revenue(best_distribution, initial_data)

output_df = generate_output_dataframe(
    results_conversion=results_conversion, results_revenue=results_revenue
)

print(f"For client {client_name} data follows {best_distribution} distribution.")
print(output_df)
