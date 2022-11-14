from typing import Literal

DISTRIBUTIONS: list = ["expon", "lognorm"]

# To-do: this actually never propagates correctly into calculating distributions
target_col: str = "total_wins_spend"
client_name: Literal[
    "bingo_aloha",
    "terra_genesis",
    "spongebob",
    "ultimex",
    "knighthood",
    "idle_mafia",
    "homw",
    "terra_2",
] = "ultimex"
