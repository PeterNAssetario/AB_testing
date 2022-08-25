from ab_testing.constants import client, target_col
from ab_testing.data_acquisition.acquire_data import AcquireBingoALohaData
from ab_testing.distribution_fit.fit_distribution import FitDistribution
from ab_testing.results.produce_results import ProduceResults

if client == "bingo_aloha":
    acquire_bingo_aloha_data = AcquireBingoALohaData()
    bingo_aloha_data = acquire_bingo_aloha_data.acquire_data()
else:
    raise NotImplementedError("Given client is not implemented yet.")

fit_dist = FitDistribution()
best_distribution = fit_dist.fit(bingo_aloha_data.loc[bingo_aloha_data[target_col] > 0], target_col)

result = ProduceResults()
results = result.produce(best_distribution, bingo_aloha_data)

print(f"For clinet {client} data follows {best_distribution} distribution.")
print(f"Variant {results[0]['variant']} is the better one with probability {results[0]['prob_being_best']}.")
print(f"For variant {results[0]['variant']} the expected loss is {results[0]['expected_loss']}.")
print(f"Variant {results[1]['variant']} is the better one with probability {results[1]['prob_being_best']}.")
print(f"For variant {results[1]['variant']} the expected loss is {results[1]['expected_loss']}.")
