query_bingo_aloha = """
WITH first_logins     AS (
    SELECT user_id
         , MIN(meta_date) first_login
    FROM etl__century_games_ncmgu__bingo_aloha_r3g9v.session_start
    GROUP BY user_id
    )
   , logins           AS (
    SELECT user_id
         , meta_date
         , first_login
         , test_group
    FROM etl__century_games_ncmgu__bingo_aloha_r3g9v.session_start
         INNER JOIN console.clean_abtest USING (user_id)
         LEFT JOIN  first_logins USING (user_id)
    WHERE meta_company_id = 'century-games-ncmgu'
      AND meta_project_id = 'bingo-aloha-r3g9v'
      AND first_login >= DATE '2022-07-01'
    )
   , player_spend     AS (
    SELECT test_group
         , user_id
         , meta_date
         , SUM(payments.sum_bi_payment_amount_usd) spend
    FROM etl__century_games_ncmgu__bingo_aloha_r3g9v.bi_payment payments
         INNER JOIN console.clean_abtest                        abtest USING (user_id)
    WHERE meta_company_id = 'century-games-ncmgu'
      AND meta_project_id = 'bingo-aloha-r3g9v'
    GROUP BY user_id
           , meta_date
           , test_group
    )
   , filtered_players AS (
    SELECT test_group
         , user_id
         , meta_date
         , approx_percentile(spend, 9.9E-1) OVER (PARTITION BY meta_date, test_group) percentile
    FROM player_spend
    )
   , wins_spend_table AS (
    SELECT user_id
         , meta_date
         , filtered_players.test_group
         , spend
         , (CASE WHEN (spend > percentile) THEN percentile ELSE spend END) wins_spend
    FROM (player_spend ps
         INNER JOIN filtered_players USING (user_id, meta_date))
    )
   , t_out            AS (
    SELECT user_id
         , test_group
         , SUM(spend)       total_spend
         , SUM(wins_spend)  total_wins_spend
    FROM logins
         LEFT JOIN wins_spend_table USING (user_id, meta_date, test_group)
    GROUP BY user_id
           , test_group
    )
SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
"""


query_bingo_aloha_30 = """
WITH first_logins     AS (
    SELECT user_id
         , MIN(meta_date) first_login
    FROM etl__century_games_ncmgu__bingo_aloha_r3g9v.session_start
    GROUP BY user_id
    )
   , logins           AS (
    SELECT user_id
         , meta_date
         , first_login
         , test_group
    FROM etl__century_games_ncmgu__bingo_aloha_r3g9v.session_start
         INNER JOIN console.clean_abtest USING (user_id)
         LEFT JOIN  first_logins USING (user_id)
    WHERE meta_company_id = 'century-games-ncmgu'
      AND meta_project_id = 'bingo-aloha-r3g9v'
      AND first_login >= CURRENT_DATE - INTERVAL '32' DAY
    )
   , player_spend     AS (
    SELECT test_group
         , user_id
         , meta_date
         , SUM(payments.sum_bi_payment_amount_usd) spend
    FROM etl__century_games_ncmgu__bingo_aloha_r3g9v.bi_payment payments
         INNER JOIN console.clean_abtest                        abtest USING (user_id)
    WHERE meta_company_id = 'century-games-ncmgu'
      AND meta_project_id = 'bingo-aloha-r3g9v'
    GROUP BY user_id
           , meta_date
           , test_group
    )
   , filtered_players AS (
    SELECT test_group
         , user_id
         , meta_date
         , approx_percentile(spend, 9.9E-1) OVER (PARTITION BY meta_date, test_group) percentile
    FROM player_spend
    )
   , wins_spend_table AS (
    SELECT user_id
         , meta_date
         , filtered_players.test_group
         , spend
         , (CASE WHEN (spend > percentile) THEN percentile ELSE spend END) wins_spend
    FROM (player_spend ps
         INNER JOIN filtered_players USING (user_id, meta_date))
    )
   , t_out            AS (
    SELECT user_id
         , test_group
         , SUM(spend)       total_spend
         , SUM(wins_spend)  total_wins_spend
    FROM logins
         LEFT JOIN wins_spend_table USING (user_id, meta_date, test_group)
    GROUP BY user_id
           , test_group
    )
SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
"""
