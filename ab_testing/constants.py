from typing import Dict, Union, Literal

from scipy import stats

ScipyDists = Union[
    stats._continuous_distns.expon_gen,
    stats._continuous_distns.lognorm_gen,
    stats._continuous_distns.norm_gen,
    stats._continuous_distns.gamma_gen,
]

DISTRIBUTIONS: Dict[str, ScipyDists] = {
    "expon": stats.expon,
    "lognorm": stats.lognorm,
    "norm": stats.norm,
    "gamma": stats.gamma,
}


target_col: str = "total_wins_spend"
client_name: Literal[
    "bingo_aloha",
    "terra_genesis",
    "spongebob",
    "ultimex",
    "idle_mafia",
    "homw",
] = "idle_mafia"