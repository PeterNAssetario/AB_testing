query_bingo_aloha = """
SELECT *
FROM analytics__century_games_ncmgu__bingo_aloha_r3g9v.user_level_performance
"""


query_terra_genesis = """
SELECT *
FROM analytics__tilting_point_mjs4k__terragenesis_m89uz.user_level_performance
"""


query_spongebob = """
SELECT *
FROM analytics__tilting_point_mjs4k__spongebob_x7d9q.user_level_performance
"""


query_ultimex = """
SELECT *
FROM analytics__sparkgaming_vjv6s__ultimate_x_poker_rib6t.user_level_performance
"""


query_knighthood = """
SELECT *
FROM analytics__phoenix_games_cd8wx__knighthood_ogh3l.user_level_performance
"""


query_idle_mafia = """
SELECT *
FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_before_1_6_2022
UNION
SELECT *
FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_after_1_6_2022
"""


query_homw = """
SELECT *
FROM analytics__tinysoft_a9kwp__heroes_magic_war_h2sln.user_level_performance
"""