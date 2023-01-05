from typing import List, Tuple
from numbers import Number

from bayesian_testing.metrics import eval_bernoulli_agg
from bayesian_testing.utilities import get_logger
from bayesian_testing.experiments.base import BaseDataTest

logger = get_logger("bayesian_testing")


class BinaryDataTest(BaseDataTest):
    """
    Class for Bayesian A/B test for binary-like data (conversions, successes, ...).

    After class initialization, use add_variant methods to insert variant data.
    Then to get results of the test, use for instance `evaluate` method.
    """

    def __init__(self) -> None:
        """
        Initialize BinaryDataTest class.
        """
        super().__init__()

    @property
    def totals(self):
        return [self.data[k]["totals"] for k in self.data]

    @property
    def positives(self):
        return [self.data[k]["positives"] for k in self.data]

    @property
    def a_priors(self):
        return [self.data[k]["a_prior"] for k in self.data]

    @property
    def b_priors(self):
        return [self.data[k]["b_prior"] for k in self.data]

    def eval_simulation(
        self, sim_count: int = 20000, seed: int = None
    ) -> Tuple[dict, dict]:
        """
        Calculate probabilities of being best and expected loss for a current class state.

        Parameters
        ----------
        sim_count : Number of simulations to be used for probability estimation.
        seed : Random seed.

        Returns
        -------
        res_pbbs : Dictionary with probabilities of being best for all variants in experiment.
        res_loss : Dictionary with expected loss for all variants in experiment.
        """
        pbbs, loss, total_gain = eval_bernoulli_agg(
            self.totals, self.positives, self.a_priors, self.b_priors, sim_count, seed
        )
        res_pbbs = dict(zip(self.variant_names, pbbs))
        res_loss = dict(zip(self.variant_names, loss))
        res_total_gain = dict(zip(self.variant_names, total_gain))

        return res_pbbs, res_loss, res_total_gain

    def evaluate(self, sim_count: int = 20000, seed: int = None) -> List[dict]:
        """
        Evaluation of experiment.

        Parameters
        ----------
        sim_count : Number of simulations to be used for probability estimation.
        seed : Random seed.

        Returns
        -------
        res : List of dictionaries with results per variant.
        """
        keys = [
            "variant",
            "totals",
            "positives",
            "positive_rate",
            "prob_being_best",
            "expected_loss",
            "expected_total_gain",
        ]

        positive_rate = [
            round(i[0] / i[1], 5) for i in zip(self.positives, self.totals)
        ]
        eval_pbbs, eval_loss, eval_total_gain = self.eval_simulation(sim_count, seed)
        pbbs = list(eval_pbbs.values())
        loss = list(eval_loss.values())
        total_gain = list(eval_total_gain.values())
        data = [
            self.variant_names,
            self.totals,
            self.positives,
            positive_rate,
            pbbs,
            loss,
            total_gain,
        ]
        res = [dict(zip(keys, item)) for item in zip(*data)]

        return res

    def add_variant_data_agg(
        self,
        name: str,
        totals: int,
        positives: int,
        a_prior: float = 0.5,
        b_prior: float = 0.5,
    ) -> None:
        """
        Add variant data to test class using aggregated binary data.
        This can be convenient as aggregation can be done on database level.

        Default prior setup is set for Beta(1/2, 1/2) which is non-information prior.

        Parameters
        ----------
        name : Variant name.
        totals : Total number of experiment observations (e.g. number of sessions).
        positives : Total number of ones for a given variant (e.g. number of conversions).
        a_prior : Prior alpha parameter for Beta distributions.
            Default value 0.5 is based on non-information prior Beta(0.5, 0.5).
        b_prior : Prior beta parameter for Beta distributions.
            Default value 0.5 is based on non-information prior Beta(0.5, 0.5).
        """
        if not isinstance(name, str):
            raise ValueError("Variant name has to be a string.")
        if a_prior <= 0 or b_prior <= 0:
            raise ValueError("Both [a_prior, b_prior] have to be positive numbers.")
        if totals <= 0:
            raise ValueError(
                "Input variable 'totals' is expected to be positive integer."
            )
        if positives < 0:
            raise ValueError(
                "Input variable 'positives' is expected to be non-negative integer."
            )
        if totals < positives:
            raise ValueError("Not possible to have more positives that totals!")

        if name in self.variant_names:
            msg = f"Variant {name} already exists - new data is replacing it. "
            logger.info(msg)

        self.data[name] = {
            "totals": totals,
            "positives": positives,
            "a_prior": a_prior,
            "b_prior": b_prior,
            "a_posterior": positives + a_prior,
            "b_posterior": totals - positives + b_prior,
        }

    def add_variant_data(
        self,
        name: str,
        data: List[int],
        a_prior: float = 0.5,
        b_prior: float = 0.5,
    ) -> None:
        """
        Add variant data to test class using raw binary data.

        Default prior setup is set for Beta(1/2, 1/2) which is non-information prior.

        Parameters
        ----------
        name : Variant name.
        data : List of binary data containing zeros (non-conversion) and ones (conversions).
        a_prior : Prior alpha parameter for Beta distributions.
            Default value 0.5 is based on non-information prior Beta(0.5, 0.5).
        b_prior : Prior beta parameter for Beta distributions.
            Default value 0.5 is based on non-information prior Beta(0.5, 0.5).
        """
        if len(data) == 0:
            raise ValueError("Data of added variant needs to have some observations.")
        if not min([i in [0, 1] for i in data]):
            raise ValueError("Input data needs to be a list of zeros and ones.")

        totals = len(data)
        positives = sum(data)

        self.add_variant_data_agg(name, totals, positives, a_prior, b_prior)
