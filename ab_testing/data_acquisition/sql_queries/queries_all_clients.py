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


query_terra_genesis = """
WITH first_logins               AS (
    SELECT account_id
         , MIN(meta_date) first_login
    FROM etl__tilting_point_mjs4k__terragenesis_m89uz.session_start
    GROUP BY account_id
    )
   , logins                     AS (
    SELECT account_id
         , meta_date
         , MIN(first_login) first_login
    FROM etl__tilting_point_mjs4k__terragenesis_m89uz.session_start
         LEFT JOIN first_logins USING (account_id)
    WHERE first_login >= CURRENT_DATE - INTERVAL '32' DAY
    GROUP BY account_id
           , meta_date
    )
   , spend_table                AS (
    SELECT account_id
         , meta_date
         , SUM(sum_value) spend
    FROM etl__tilting_point_mjs4k__terragenesis_m89uz.purchase
         LEFT JOIN first_logins USING (account_id)
    GROUP BY account_id
           , meta_date
    )
   , filtered_players           AS (
    SELECT account_id
         , meta_date
         , approx_percentile(spend, 9.9E-1) OVER (PARTITION BY meta_date) percentile
    FROM spend_table
    )
   , wins_spend_table           AS (
    SELECT account_id
         , meta_date
         , spend
         , (CASE WHEN (spend > percentile) THEN percentile ELSE spend END) wins_spend
    FROM (spend_table ps
         INNER JOIN filtered_players USING (account_id, meta_date))
    )
   , date_purchase_given_bucket AS (
    SELECT account_id
         , meta_date
         , compliance_area
         , manufacturer
         , region_tier
         , device_tier
         , predicted_value
         , group_tag
         , spend
         , wins_spend
    FROM logins
         RIGHT JOIN analytics__tilting_point_mjs4k__terragenesis_m89uz.bucket_assignment_map USING (account_id)
         LEFT JOIN  wins_spend_table USING (account_id, meta_date)
    )
   , t_out                      AS (
    SELECT account_id      user_id
         , group_tag       test_group
         , SUM(spend)      total_spend
         , SUM(wins_spend) total_wins_spend
    FROM date_purchase_given_bucket
    GROUP BY account_id
           , group_tag
    )
SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
"""


query_spongebob = """
WITH first_logins               AS (
    SELECT account_id
         , MIN(meta_date) first_login
    FROM etl__tilting_point_mjs4k__spongebob_x7d9q.session_start
    GROUP BY account_id
    )
   , logins                     AS (
    SELECT account_id
         , meta_date
    FROM (etl__tilting_point_mjs4k__spongebob_x7d9q.session_start
        LEFT JOIN first_logins USING (account_id)
             )
    WHERE first_login >= CURRENT_DATE - INTERVAL '32' DAY
    GROUP BY account_id
           , meta_date
    )
   , spend_table                AS (
    SELECT account_id
         , meta_date
         , SUM(purchase_sum_value) spend
    FROM etl__tilting_point_mjs4k__spongebob_x7d9q.purchase
         LEFT JOIN first_logins USING (account_id)
    GROUP BY account_id
           , meta_date
    )
   , filtered_players           AS (
    SELECT account_id
         , meta_date
         , approx_percentile(spend, 9.9E-1) OVER (PARTITION BY meta_date) percentile
    FROM spend_table
    )
   , wins_spend_table           AS (
    SELECT account_id
         , meta_date
         , spend
         , (CASE WHEN (spend > percentile) THEN percentile ELSE spend END) wins_spend
    FROM (spend_table ps
         INNER JOIN filtered_players USING (account_id, meta_date))
    )
   , date_purchase_given_bucket AS (
    SELECT account_id
         , meta_date
         , compliance_area
         , is_restricted_country
         , manufacturer
         , region_tier
         , device_tier
         , predicted_value
         , group_tag
         , spend
         , wins_spend
    FROM logins
         RIGHT JOIN analytics__tilting_point_mjs4k__spongebob_x7d9q.bucket_assignment_map USING (account_id)
         LEFT JOIN  wins_spend_table USING (account_id, meta_date)
    )
   , t_out                      AS (
    SELECT account_id      user_id
         , group_tag       test_group
         , SUM(spend)      total_spend
         , SUM(wins_spend) total_wins_spend
    FROM date_purchase_given_bucket
    GROUP BY account_id
           , group_tag
    )
SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
"""


query_ultimex = """
WITH
    first_logins     AS (
        SELECT user_id
             , MIN(meta_date) first_login
        FROM etl__sparkgaming_vjv6s__ultimate_x_poker_rib6t.session_start logins
        GROUP BY user_id
        )

  , login_stats      AS (
    SELECT user_id
         , MAX(test_group)  AS test_group
         , MIN(first_login) AS first_login
    FROM etl__sparkgaming_vjv6s__ultimate_x_poker_rib6t.session_start
             INNER JOIN console.clean_abtest
                        USING (user_id)
             LEFT JOIN  first_logins
                        USING (user_id)
    WHERE meta_company_id = 'sparkgaming-vjv6s'
      AND meta_project_id = 'ultimate-x-poker-rib6t'
      and session_start.meta_date >= (DATE ('2022-10-13'))
       AND first_login >= (DATE ('2022-10-13'))
    GROUP BY user_id
    )

  , targeted_offers  AS (
    SELECT payments.user_id        user_id
         , SUM(payments.sum_value) sum_value
    FROM etl__sparkgaming_vjv6s__ultimate_x_poker_rib6t.purchase          payments
             LEFT JOIN console."sparkgaming_ultimate-x-poker_offer_store" offers
                       ON (payments.event_params_item_id = offers.product_id)
    WHERE 1 = 1
      AND payments.meta_date >= (DATE ('2022-10-13'))
      -- AND offer_type LIKE '%Coin%'
    GROUP BY user_id
    )

  , player_spend     AS (
    SELECT user_id
         , test_group
         , SUM(sum_value) spend
    FROM targeted_offers
             INNER JOIN console.clean_abtest abtest
                        USING (user_id)
             INNER JOIN first_logins
                        USING (user_id)
    WHERE meta_company_id = 'sparkgaming-vjv6s'
      AND meta_project_id = 'ultimate-x-poker-rib6t'
    GROUP BY user_id
           , test_group
    )

  , filtered_players AS (
    SELECT user_id
         , test_group
         , approx_percentile(spend, 9.9E-1) OVER (partition BY test_group) percentile
    FROM player_spend
    )

  , wins_spend_table AS (
    SELECT user_id
         , test_group
         , spend
         , (CASE
                WHEN (spend > percentile) THEN percentile
                ELSE spend
            END) wins_spend
    FROM player_spend ps
             INNER JOIN filtered_players
                        USING (user_id, test_group)
    )

  , t_out            AS (
    SELECT user_id
         , test_group
         , round(SUM(spend), 2)      total_spend
         , round(SUM(wins_spend), 2) total_wins_spend
    FROM login_stats                    t1
             LEFT JOIN wins_spend_table t2
                       USING (user_id, test_group)
    GROUP BY user_id
           , test_group
    )

SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
ORDER BY total_spend DESC
"""


query_knighthood = """
WITH first_logins     AS (
    SELECT userid
         , MIN(meta_date) first_login
    FROM etl__phoenix_games_cd8wx__knighthood_ogh3l.metric_login
    GROUP BY userid
    )
   , purchases        AS (
    SELECT userid
         , SUM(sum_metric_iap_dollars) sum_purchases
         , meta_date
    FROM etl__phoenix_games_cd8wx__knighthood_ogh3l.metric_iap payments
    GROUP BY userid
           , meta_date
    )
   , logins           AS (
    SELECT userid
         , logins.meta_date
         , (CASE
                WHEN (experimentgroup = 0) THEN 'Control'
                WHEN (experimentgroup = 1) THEN 'Revised Deals'
                WHEN (experimentgroup = 2) THEN 'Assetario'
                ELSE 'None' END) test_group
    FROM etl__phoenix_games_cd8wx__knighthood_ogh3l.metric_login                      logins
         RIGHT JOIN etl__phoenix_games_cd8wx__knighthood_ogh3l.metric_assign_ab_group abtest USING (userid)
         LEFT JOIN  first_logins                                                      fr_log USING (userid)
    WHERE first_login >= DATE '2022-03-26'
      AND experiment = 'Deal_Revison_Experiment'
    GROUP BY userid
           , logins.meta_date
           , experimentgroup
    )
   , filtered_players AS (
    SELECT userid
         , test_group
         , COALESCE(purchases.meta_date, logins.meta_date) meta_date
         , approx_percentile(sum_purchases, 9.9E-1)        OVER (PARTITION BY COALESCE(purchases.meta_date, logins.meta_date), test_group) percentile
    FROM logins
         LEFT JOIN purchases USING (userid)
    )
   , wins_spend_table AS (
    SELECT userid
         , test_group
         , meta_date
         , sum_purchases                                                                   spend
         , (CASE WHEN (sum_purchases > percentile) THEN percentile ELSE sum_purchases END) wins_spend
    FROM (purchases ps
         INNER JOIN filtered_players USING (userid, meta_date))
    )
   , t_out            AS (
    SELECT userid          user_id
         , test_group
         , SUM(spend)      total_spend
         , SUM(wins_spend) total_wins_spend
    FROM logins
         LEFT JOIN wins_spend_table USING (userid, test_group)
    GROUP BY userid
           , test_group
    )
SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
"""


query_idle_mafia = """
WITH first_logins      AS (
    SELECT account_id
         , MIN(meta_date) first_login
    FROM etl__century_games_ncmgu__idle_mafia_ecbqb.login_stats logins
    GROUP BY account_id
    )
   , player_spend      AS (
    SELECT test_group
         , account_id
         , meta_date
         , SUM(payments.sum_purchases_package_key_daily) spend
    FROM etl__century_games_ncmgu__idle_mafia_ecbqb.shop_package_key_daily_purchase_popularity_stats payments
         INNER JOIN console.clean_abtest                                                             abtest ON (payments.account_id = abtest.user_id)
    WHERE abtest.meta_project_id = 'idle-mafia-ecbqb'
      AND abtest.meta_company_id = 'century-games-ncmgu'
    GROUP BY account_id
           , meta_date
           , test_group
    )
   , filtered_players  AS (
    SELECT test_group
         , account_id
         , meta_date
         , approx_percentile(spend, 9.9E-1) OVER (PARTITION BY meta_date, test_group) percentile
    FROM player_spend
    )
   , wins_spend_table  AS (
    SELECT account_id
         , meta_date
         , filtered_players.test_group
         , spend
         , (CASE WHEN (spend > percentile) THEN percentile ELSE spend END) wins_spend
    FROM player_spend ps
         INNER JOIN filtered_players USING (account_id, meta_date)
    )
   , logins_with_group AS (
    SELECT account_id
         , meta_date
         , test_group
    FROM etl__century_games_ncmgu__idle_mafia_ecbqb.login_stats logins
         INNER JOIN console.clean_abtest                        abtest ON (logins.account_id = abtest.user_id)
         LEFT JOIN  first_logins USING (account_id)
    WHERE abtest.meta_project_id = 'idle-mafia-ecbqb'
      AND abtest.meta_company_id = 'century-games-ncmgu'
      AND first_login >= CURRENT_DATE - INTERVAL '32' DAY
    )
   , t_out             AS (
    SELECT account_id       user_id
         , test_group
         , SUM(spend)       total_spend
         , SUM(wins_spend)  total_wins_spend
    FROM logins_with_group
         LEFT JOIN wins_spend_table USING (account_id, test_group)
    GROUP BY account_id
           , test_group
    )
SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
"""


query_homw = """

WITH

    min_logins       AS (
        SELECT user_id
             , MIN(meta_date) AS min_login
        FROM etl__tinysoft_a9kwp__heroes_magic_war_h2sln.session_start log
        GROUP BY 1
        )

  , logins           AS (
    SELECT user_id
         , test_group
    FROM etl__tinysoft_a9kwp__heroes_magic_war_h2sln.session_start log
             LEFT JOIN (
                           SELECT user_id
                                , MAX(test_group) AS test_group
                           FROM console.clean_abtest
                           WHERE meta_company_id = 'tinysoft-a9kwp'
                             AND meta_project_id = 'heroes-magic-war-h2sln'
                           GROUP BY user_id
                           )
                       USING (user_id)
    WHERE 1 = 1
      AND meta_date >= (DATE ('2022-09-23'))
      AND user_id NOT IN ('0000-0000', '00000000-0000-0000-0000-000000000000')
    GROUP BY user_id
           , test_group
    )
  , purchases        AS (
    SELECT user_id
         , SUM(sum_in_app_purchase_event_value_in_usd) spend
         , MAX(test_group)                             test_group
    FROM etl__tinysoft_a9kwp__heroes_magic_war_h2sln.in_app_purchase
             LEFT JOIN (
                           SELECT user_id
                                , MAX(test_group) AS test_group
                           FROM console.clean_abtest
                           WHERE meta_company_id = 'tinysoft-a9kwp'
                             AND meta_project_id = 'heroes-magic-war-h2sln'
                           GROUP BY user_id
                           )
                       USING (user_id)
    WHERE 1 = 1
      AND meta_date >= (DATE ('2022-09-23'))
    GROUP BY user_id
    )
  , filtered_players AS (
    SELECT test_group
         , user_id
         , approx_percentile(spend, 9.9E-1) OVER (partition BY test_group) percentile
    FROM purchases
    )
  , wins_spend_table AS (
    SELECT user_id
         , filtered_players.test_group
         , spend
         , (CASE WHEN (spend > percentile) THEN percentile ELSE spend END) wins_spend
    FROM purchases ps
             INNER JOIN filtered_players
                        USING (user_id)
    )
  , t_out            AS (
    SELECT user_id
         , test_group
         , SUM(spend)      total_spend
         , SUM(wins_spend) total_wins_spend
    FROM logins
             LEFT JOIN wins_spend_table
                       USING (user_id, test_group)
             LEFT JOIN min_logins
                       USING (user_id)
    WHERE min_login >= (DATE ('2022-09-23'))
    GROUP BY user_id
           , test_group
    )
SELECT user_id
     , test_group
     , COALESCE(total_spend, 0)      total_spend
     , COALESCE(total_wins_spend, 0) total_wins_spend
FROM t_out
ORDER BY total_spend DESC
"""


query_terra_2 = """
WITH
    test   AS (
        SELECT account_id
             , experiment_id
             , CASE
                   WHEN experiment_variant_id LIKE '5830%' THEN 'C'
                   WHEN experiment_variant_id LIKE '5580%' THEN 'P'
                   ELSE 'other'
               END AS test_group
        FROM (
                 SELECT account_id
                      , experiment_id
                      , experiment_variant_id
                      , meta_date
                      , rank() over (partition BY account_id ORDER BY meta_date DESC) AS rank_
                 FROM etl__tilting_point_mjs4k__terragenesis_m89uz.experiments
                 WHERE 1 = 1
                   AND experiment_id = '5133572907270144'
                   AND meta_date >= (DATE ('2022-07-05'))
                 )
        WHERE rank_ = 1
        )

  , logins AS (
    SELECT *
         , CASE
               WHEN min_date BETWEEN (DATE ( '2022-07-05')) AND (DATE ( '2022-07-11')) THEN '1'
               WHEN min_date BETWEEN (DATE ( '2022-07-13')) AND (DATE ( '2022-07-23')) THEN '2'
               WHEN min_date BETWEEN (DATE ( '2022-07-25')) AND (DATE ( '2022-08-07')) THEN '3'
               WHEN min_date BETWEEN (DATE ( '2022-08-09')) AND (DATE ( '2022-08-17')) THEN '4_broken_control'
               WHEN min_date BETWEEN (DATE ('2022-08-19')) AND (DATE ( '2022-09-01'))  THEN '5'
               WHEN min_date >= (DATE ('2022-09-03'))                                  THEN '6'
               ELSE '0'
           END AS cohort
    FROM (
             SELECT account_id
                  , MIN(meta_date) AS min_date
             FROM etl__tilting_point_mjs4k__terragenesis_m89uz.session_start
             GROUP BY account_id
             )
    WHERE 1 = 1
      AND min_date >= (DATE ('2022-07-05'))
    )

  , spend  AS (
    SELECT account_id
         , SUM(sum_value) AS sum_spend
    FROM etl__tilting_point_mjs4k__terragenesis_m89uz.purchase
    WHERE 1 = 1
      AND meta_date >= (DATE ('2022-07-05'))
    GROUP BY account_id
    )

  , preds  AS (
    SELECT MAX(CAST(json_extract(metadata, '$.region_tier') AS VARCHAR))    AS region_tier
         , MAX(CAST(json_extract(metadata, '$.device_tier') AS VARCHAR))    AS device_tier
         , MAX(predicted_value)                                             AS pred_value
         , MAX(CAST(json_extract(metadata, '$.prediction_tag') AS VARCHAR)) AS tag
         , user_id                                                          AS account_id
    FROM platform__tilting_point_mjs4k__terragenesis_m89uz.prediction_logs
    WHERE 1 = 1
    GROUP BY user_id
    )

SELECT test_group
     , account_id AS user_id
     , coalesce(sum_spend,0)  AS total_spend
     , coalesce(sum_spend,0)  AS total_wins_spend
FROM test
         INNER JOIN logins
                    USING (account_id)
         INNER JOIN preds
                    USING (account_id)
         LEFT JOIN  spend
                    USING (account_id)
WHERE 1 = 1
  AND tag = 'apple_success_prediction'
  AND cohort IN ('4_broken_control', '5', '6')
;
"""
