from ab_testing.constants import target_col
from ab_testing.data_acquisition.acquire_data import AcquireBingoALohaData
from ab_testing.distribution_fit.fit_distribution import FitDistribution
from ab_testing.results.produce_results import ProduceResults

acquire_bingo_aloha_data = AcquireBingoALohaData()
bingo_aloha_data = acquire_bingo_aloha_data.acquire_data()

fit_dist = FitDistribution()
best_distribution = fit_dist.fit(bingo_aloha_data.loc[bingo_aloha_data[target_col] > 0], target_col)

result = ProduceResults()
results = result.produce(best_distribution, bingo_aloha_data)
import pdb

pdb.set_trace()
