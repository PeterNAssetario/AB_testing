query_bingo_aloha = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN spend END), 0)      total_spend
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN wins_spend END), 0) total_wins_spend
FROM analytics__century_games_ncmgu__bingo_aloha_r3g9v.user_level_performance
WHERE meta_date   BETWEEN DATE %(strt_date)s AND DATE %(end_date)s
  AND first_login BETWEEN DATE %(strt_fl)s   AND DATE %(end_fl)s
GROUP BY user_id
       , meta_date
       , first_login
       , group_tag;
"""

query_bingo_aloha_sample = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM analytics__century_games_ncmgu__bingo_aloha_r3g9v.user_level_performance
GROUP BY user_id
       , meta_date
       , first_login
       , group_tag
LIMIT 10;
"""

query_bingo_aloha_date_limits = """
SELECT MIN(meta_date) AS min_meta_date,
       MAX(meta_date) AS max_meta_date,
       MIN(first_login) AS min_first_login,
       MAX(first_login) AS max_first_login
FROM analytics__century_games_ncmgu__bingo_aloha_r3g9v.user_level_performance;
"""


query_terra_genesis = """
SELECT user_id
     , meta_date
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM analytics__tilting_point_mjs4k__terragenesis_m89uz.user_level_performance
WHERE meta_date   BETWEEN DATE %(strt_date)s AND DATE %(end_date)s
  AND first_login BETWEEN DATE %(strt_fl)s   AND DATE %(end_fl)s
GROUP BY user_id
       , group_tag
       , meta_date;
"""

query_terra_genesis_sample = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM analytics__tilting_point_mjs4k__terragenesis_m89uz.user_level_performance
GROUP BY user_id
       , group_tag
       , meta_date
       , first_login
LIMIT 10;
"""

query_terra_genesis_date_limits = """
SELECT MIN(meta_date) AS min_meta_date,
       MAX(meta_date) AS max_meta_date,
       MIN(first_login) AS min_first_login,
       MAX(first_login) AS max_first_login
FROM analytics__tilting_point_mjs4k__terragenesis_m89uz.user_level_performance;
"""

query_spongebob = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM analytics__tilting_point_mjs4k__spongebob_x7d9q.user_level_performance
WHERE meta_date   BETWEEN DATE %(strt_date)s AND DATE %(end_date)s
  AND first_login BETWEEN DATE %(strt_fl)s   AND DATE %(end_fl)s
GROUP BY user_id
       , meta_date
       , group_tag
       , first_login;
"""

query_spongebob_sample = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM analytics__tilting_point_mjs4k__spongebob_x7d9q.user_level_performance
GROUP BY user_id
       , meta_date
       , first_login
       , group_tag
LIMIT 10;
"""

query_spongebob_date_limits = """
SELECT MIN(meta_date) AS min_meta_date,
       MAX(meta_date) AS max_meta_date,
       MIN(first_login) AS min_first_login,
       MAX(first_login) AS max_first_login
FROM analytics__tilting_point_mjs4k__spongebob_x7d9q.user_level_performance;
"""

query_ultimex = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN spend END), 0)      total_spend
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN wins_spend END), 0) total_wins_spend
FROM analytics__sparkgaming_vjv6s__ultimate_x_poker_rib6t.user_level_performance
WHERE meta_date   BETWEEN DATE %(strt_date)s AND DATE %(end_date)s
  AND first_login BETWEEN DATE %(strt_fl)s   AND DATE %(end_fl)s
GROUP BY user_id
       , group_tag
       , first_login
       , meta_date;
"""

query_ultimex_sample = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM analytics__sparkgaming_vjv6s__ultimate_x_poker_rib6t.user_level_performance 
GROUP BY user_id
       , group_tag
       , meta_date
       , first_login
LIMIT 100;
"""

query_ultimex_date_limits = """
SELECT MIN(meta_date) AS min_meta_date,
       MAX(meta_date) AS max_meta_date,
       MIN(first_login) AS min_first_login,
       MAX(first_login) AS max_first_login
FROM analytics__sparkgaming_vjv6s__ultimate_x_poker_rib6t.user_level_performance;
"""


query_homw = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN spend END), 0)      total_spend
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN wins_spend END), 0) total_wins_spend
FROM analytics__tinysoft_a9kwp__heroes_magic_war_h2sln.user_level_performance
WHERE meta_date   BETWEEN DATE %(strt_date)s AND DATE %(end_date)s
  AND first_login BETWEEN DATE %(strt_fl)s   AND DATE %(end_fl)s
GROUP BY user_id
       , group_tag
       , first_login
       , meta_date;
"""

query_homw_sample = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM analytics__tinysoft_a9kwp__heroes_magic_war_h2sln.user_level_performance
GROUP BY user_id
       , group_tag
       , meta_date
       , first_login
LIMIT 100;
"""

query_homw_date_limits = """
SELECT MIN(meta_date) AS min_meta_date,
       MAX(meta_date) AS max_meta_date,
       MIN(first_login) AS min_first_login,
       MAX(first_login) AS max_first_login
FROM analytics__tinysoft_a9kwp__heroes_magic_war_h2sln.user_level_performance;
"""

query_idle_mafia = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN spend END), 0)      total_spend
     , COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> %(spend_type)s THEN wins_spend END), 0) total_wins_spend
FROM (
     SELECT user_id, meta_date, first_login, spend, wins_spend, group_tag, os, fl_personalized_offer_spend
     FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_before_1_6_2022
     UNION
     SELECT user_id, meta_date, first_login, spend, wins_spend, group_tag, os, fl_personalized_offer_spend
     FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_after_1_6_2022
     )
FROM analytics__tinysoft_a9kwp__heroes_magic_war_h2sln.user_level_performance
WHERE meta_date   BETWEEN DATE %(strt_date)s AND DATE %(end_date)s
  AND first_login BETWEEN DATE %(strt_fl)s   AND DATE %(end_fl)s
GROUP BY user_id
       , group_tag
       , meta_date
       , first_login;
"""

query_idle_mafia_sample = """
SELECT user_id
     , meta_date
     , first_login
     , CASE
           WHEN group_tag = 'control' THEN 'C'
           WHEN group_tag = 'personalized' THEN 'P'
    END                             test_group
     , COALESCE(SUM(spend), 0)      total_spend
     , COALESCE(SUM(wins_spend), 0) total_wins_spend
FROM (
     SELECT user_id, meta_date, first_login, spend, wins_spend, group_tag, os, fl_personalized_offer_spend
     FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_before_1_6_2022
     UNION
     SELECT user_id, meta_date, first_login, spend, wins_spend, group_tag, os, fl_personalized_offer_spend
     FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_after_1_6_2022
     )
GROUP BY user_id
       , group_tag
       , meta_date
       , first_login
LIMIT 100;
"""

query_idle_mafia_date_limits = """
SELECT MIN(meta_date) AS min_meta_date,
       MAX(meta_date) AS max_meta_date,
       MIN(first_login) AS min_first_login,
       MAX(first_login) AS max_first_login
FROM (
     SELECT user_id, meta_date, first_login, spend, wins_spend, group_tag, os, fl_personalized_offer_spend
     FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_before_1_6_2022
     UNION
     SELECT user_id, meta_date, first_login, spend, wins_spend, group_tag, os, fl_personalized_offer_spend
     FROM analytics__century_games_ncmgu__idle_mafia_ecbqb.user_level_performance_after_1_6_2022
     );
"""
