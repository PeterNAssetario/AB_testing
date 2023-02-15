import pdb
from typing import List, Tuple, Union
from numbers import Number

import numpy as np

from bayesian_testing.utilities import get_logger
from bayesian_testing.metrics.posteriors import (
    normal_posteriors,
    beta_posteriors_all,
    lognormal_posteriors,
)

logger = get_logger("bayesian_testing")


def validate_bernoulli_input(totals: List[int], positives: List[int]) -> None:
    """
    Simple validation for pbb_bernoulli_agg inputs.
    """
    if len(totals) != len(positives):
        msg = (
            f"Totals ({totals}) and positives ({positives}) needs to have same length!"
        )
        logger.error(msg)
        raise ValueError(msg)


def estimate_probabilities(data: Union[List[List[float]], np.ndarray]) -> List[float]:
    """
    Estimate probabilities for variants considering simulated data from respective posteriors.
    Parameters
    ----------
    data : List of simulated data for each variant.
    Returns
    -------
    res : List of probabilities of being best for each variant.
    """
    max_values = np.argmax(data, axis=0)
    unique, counts = np.unique(max_values, return_counts=True)
    occurrences = dict(zip(unique, counts))
    sim_count = len(data[0])
    res = []
    for i in range(len(data)):
        res.append(round(occurrences.get(i, 0) / sim_count, 7))
    return res


def estimate_expected_loss(data: Union[List[List[float]], np.ndarray]) -> List[float]:
    """
    Estimate expected losses for variants considering simulated data from respective posteriors.
    Parameters
    ----------
    data : List of simulated data for each variant.
    Returns
    -------
    res : List of expected loss for each variant.
    """
    max_values = np.max(data, axis=0)
    res = list(np.mean(max_values - data, axis=1).round(7))
    return res


# TODO: For now this only works for the case of two variants and not more
def estimate_expected_total_gain(
    data: Union[List[List[float]], np.ndarray]
) -> List[float]:
    """
    Estimate expected total gain for variants considering simulated data from respective posteriors.
    Parameters
    ----------
    data : List of simulated data for each variant.
    Returns
    -------
    res : List of expected total gains for each variant.
    """
    # pdb.set_trace()
    res = list(np.mean(data - data[[1, 0], :], axis=1).round(7))
    return res


def eval_bernoulli_agg(
    totals: List[int],
    positives: List[int],
    a_priors_beta: List[float] = None,
    b_priors_beta: List[float] = None,
    sim_count: int = 20000,
    seed: int = None,
) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    """
    Method estimating probabilities of being best and expected loss for beta-bernoulli
    aggregated data per variant.
    Parameters
    ----------
    totals : List of numbers of experiment observations (e.g. number of sessions) for each variant.
    positives : List of numbers of ones (e.g. number of conversions) for each variant.
    sim_count : Number of simulations to be used for probability estimation.
    a_priors_beta : List of prior alpha parameters for Beta distributions for each variant.
    b_priors_beta : List of prior beta parameters for Beta distributions for each variant.
    seed : Random seed.
    Returns
    -------
    res_pbbs : List of probabilities of being best for each variant.
    res_loss : List of expected loss for each variant.
    """
    validate_bernoulli_input(totals, positives)

    if len(totals) == 0:
        return [], []

    # Default prior for all variants is Beta(0.5, 0.5) which is non-information prior.
    if not a_priors_beta:
        a_priors_beta = [0.5] * len(totals)
    if not b_priors_beta:
        b_priors_beta = [0.5] * len(totals)

    beta_samples, a_posteriors_beta, b_posteriors_beta = beta_posteriors_all(
        totals, positives, sim_count, a_priors_beta, b_priors_beta, seed
    )

    res_pbbs = estimate_probabilities(beta_samples)
    res_loss = estimate_expected_loss(beta_samples)
    res_total_gain = estimate_expected_total_gain(beta_samples)

    return res_pbbs, res_loss, res_total_gain, a_posteriors_beta, b_posteriors_beta


def eval_delta_lognormal_agg(
    totals: List[int],
    non_zeros: List[int],
    sum_logs: List[float],
    sum_logs_2: List[float],
    sim_count: int = 20000,
    a_priors_beta: List[float] = None,
    b_priors_beta: List[float] = None,
    m_priors: List[float] = None,
    a_priors_ig: List[float] = None,
    b_priors_ig: List[float] = None,
    w_priors: List[float] = None,
    seed: int = None,
) -> Tuple[
    List[float],
    List[float],
    List[float],
    List[float],
    List[float],
    List[float],
    List[float],
    List[float],
    List[float],
]:
    """
    Method estimating probabilities of being best and expected loss for delta-lognormal
    aggregated data per variant. For that reason, the method works with both totals and non_zeros.
    Parameters
    ----------
    totals : List of numbers of experiment observations (e.g. number of sessions) for each variant.
    non_zeros : List of numbers of non-zeros (e.g. number of conversions) for each variant.
    sum_logs : List of sum of logarithms of original data for each variant.
    sum_logs_2 : List of sum of logarithms squared of original data for each variant.
    sim_count : Number of simulations.
    a_priors_beta : List of prior alpha parameters for Beta distributions for each variant.
    b_priors_beta : List of prior beta parameters for Beta distributions for each variant.
    m_priors : List of prior means for logarithms of non-zero data for each variant.
    a_priors_ig : List of prior alphas from inverse gamma dist approximating variance of logarithms.
    b_priors_ig : List of prior betas from inverse gamma dist approximating variance of logarithms.
    w_priors : List of prior effective sample sizes for each variant.
    seed : Random seed.
    Returns
    -------
    res_pbbs : List of probabilities of being best for each variant.
    res_loss : List of expected loss for each variant.
    res_total_gain: List of expected total gain for each variant.
    """
    if len(totals) == 0:
        return [], [], [], [], [], [], [], [], []
    # Same default priors for all variants if they are not provided.
    if not a_priors_beta:
        a_priors_beta = [0.5] * len(totals)
    if not b_priors_beta:
        b_priors_beta = [0.5] * len(totals)
    if not m_priors:
        m_priors = [1] * len(totals)
    if not a_priors_ig:
        a_priors_ig = [0] * len(totals)
    if not b_priors_ig:
        b_priors_ig = [0] * len(totals)
    if not w_priors:
        w_priors = [0.01] * len(totals)

    if max(non_zeros) <= 0:
        # if only zeros in all variants
        res_pbbs = list(np.full(len(totals), round(1 / len(totals), 7)))
        res_loss = [np.nan] * len(totals)
        return (
            res_pbbs,
            res_loss,
        )
    else:
        # we will need different generators for each call of lognormal_posteriors
        ss = np.random.SeedSequence(seed)
        child_seeds = ss.spawn(len(totals) + 1)

        beta_samples, a_posteriors_beta, b_posteriors_beta = beta_posteriors_all(
            totals, non_zeros, sim_count, a_priors_beta, b_priors_beta, child_seeds[0]
        )

        lognorm_samples_and_post_vals = [
            lognormal_posteriors(
                non_zeros[i],
                sum_logs[i],
                sum_logs_2[i],
                sim_count,
                m_priors[i],
                a_priors_ig[i],
                b_priors_ig[i],
                w_priors[i],
                child_seeds[1 + i],
            )
            for i in range(len(totals))
        ]

        lognorm_samples = [
            lognorm_samples_and_post_vals[i][0] for i in range(len(totals))
        ]
        a_posteriors_ig = [
            lognorm_samples_and_post_vals[i][1] for i in range(len(totals))
        ]
        b_posteriors_ig = [
            lognorm_samples_and_post_vals[i][2] for i in range(len(totals))
        ]
        w_posteriors = [lognorm_samples_and_post_vals[i][3] for i in range(len(totals))]
        m_posteriors = [lognorm_samples_and_post_vals[i][4] for i in range(len(totals))]

        # pdb.set_trace()

        combined_samples = beta_samples * lognorm_samples

        res_pbbs = estimate_probabilities(combined_samples)
        res_loss = estimate_expected_loss(combined_samples)
        res_total_gain = estimate_expected_total_gain(combined_samples)

    return (
        res_pbbs,
        res_loss,
        res_total_gain,
        a_posteriors_beta,
        b_posteriors_beta,
        a_posteriors_ig,
        b_posteriors_ig,
        w_posteriors,
        m_posteriors,
    )
