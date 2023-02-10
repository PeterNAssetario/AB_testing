import os
import pdb
from enum import Enum
from typing import Optional
from datetime import datetime, timedelta

import boto3
import pandas as pd
from ml_lib.feature_store import configure_offline_feature_store
from ml_lib.feature_store.offline.client import FeatureStoreOfflineClient

from bayesian_testing.experiments import DeltaLognormalDataTest


class PossibleCompanyIds(str, Enum):
    century_games = "century_games_ncmgu"
    tinysoft = "tinysoft_a9kwp"
    tilting_point = "tilting_point_mjs4k"
    sparkgaming = "sparkgaming_vjv6s"


class PossibleProjectIds(str, Enum):
    spongebob = "spongebob_x7d9q"
    terragenesis = "terragenesis_m89uz"
    idle_mafia = "idle_mafia_ecbqb"
    bingo_aloha = "bingo_aloha_r3g9v"
    ultimatex = "ultimate_x_poker_rib6t"
    heroes_magic_war = "heroes_magic_war_h2sln"


class PossibleDatapointTypes(str, Enum):
    one_datapoint_per_user_per_meta_date = "one_datapoint_per_user_per_meta_date"
    one_datapoint_per_user_first_n_day_spend = (
        "one_datapoint_per_user_first_n_day_spend"
    )


def run_ab_testing(
    start_date: str,
    end_date: str,
    company_id: PossibleCompanyIds,
    project_id: PossibleProjectIds,
    test_name: str,
    ab_test_id: str,
    personalized: bool,
    winsorized: bool,
    datapoint_type: PossibleDatapointTypes,
    min_first_login_date: str,
    max_first_login_date: str,
    n_days_spend: int,
    variant_name_1: str,
    variant_name_2: str,
    a_prior_beta_1: float,
    a_prior_beta_2: float,
    b_prior_beta_1: float,
    b_prior_beta_2: float,
    m_prior_1: float,
    m_prior_2: float,
    a_prior_ig_1: float,
    a_prior_ig_2: float,
    b_prior_ig_1: float,
    b_prior_ig_2: float,
    w_prior_1: float,
    w_prior_2: float,
    initial_test_start_date: str,
) -> pd.DataFrame:
    configure_offline_feature_store(workgroup="development", catalog_name="production")

    # create dict for saving the test definition
    test_definition_dict = {
        "company_id": company_id.value,
        "project_id": project_id.value,
        "test_name": test_name,
        "ab_test_id": ab_test_id,
        "intitial_test_start_date": initial_test_start_date,
        "personalized": personalized,
        "winsorized": winsorized,
        "datapoint_type": datapoint_type.value,
        "min_first_login_date": min_first_login_date,
        "max_first_login_date": max_first_login_date,
    }

    # prepare query params
    if personalized:
        personalized_num: int = 0
    else:
        personalized_num = 9

    if winsorized:
        spend_column: str = "spend"
    else:
        spend_column = "wins_spend"

    if project_id in ["spongebob_x7d9q", "terragenesis_m89uz"]:
        spending_line = f", SUM({spend_column}) as total_spend"
    else:
        spending_line = f", COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> {personalized_num} THEN {spend_column} END), 0) total_spend"

    if n_days_spend:
        spend_offset = str(n_days_spend - 1)

    # check whether it isn't too early to perform the test
    too_early_to_run_test = False
    end_date_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    initial_test_start_date_datetime = datetime.strptime(
        initial_test_start_date, "%Y-%m-%d"
    )
    if datapoint_type == PossibleDatapointTypes.one_datapoint_per_user_per_meta_date:
        if end_date_datetime < initial_test_start_date_datetime:
            too_early_to_run_test = True
    elif (
        datapoint_type
        == PossibleDatapointTypes.one_datapoint_per_user_first_n_day_spend
    ):
        if (
            end_date_datetime - timedelta(int(spend_offset))
            < initial_test_start_date_datetime
        ):
            too_early_to_run_test = True

    if too_early_to_run_test:
        inputs_dict = {
            "totals_variant_1": 0.0,
            "totals_variant_2": 0.0,
            "positives_variant_1": 0.0,
            "positives_variant_2": 0.0,
            "sum_values_variant_1": 0.0,
            "sum_values_variant_2": 0.0,
            "sum_logs_variant_1": 0.0,
            "sum_logs_variant_2": 0.0,
            "sum_logs_squared_variant_1": 0.0,
            "sum_logs_squared_variant_2": 0.0,
            "a_prior_beta_variant_1": 0.0,
            "a_prior_beta_variant_2": 0.0,
            "b_prior_beta_variant_1": 0.0,
            "b_prior_beta_variant_2": 0.0,
            "m_prior_variant_1": 0.0,
            "m_prior_variant_2": 0.0,
            "a_prior_ig_variant_1": 0.0,
            "a_prior_ig_variant_2": 0.0,
            "b_prior_ig_variant_1": 0.0,
            "b_prior_ig_variant_2": 0.0,
            "w_prior_variant_1": 0.0,
            "w_prior_variant_2": 0.0,
        }

        results_dict = {
            "too_early_to_run_test": True,
            "prob_being_best_version_1": None,
            "prob_being_best_version_2": None,
            "expected_loss_version_1": None,
            "expected_loss_version_2": None,
            "expected_total_gain_version_1": None,
            "expected_total_gain_version_2": None,
        }

        output_dict = {
            "inputs": inputs_dict,
            "name_variant_1": variant_name_1,
            "name_variant_2": variant_name_2,
            "a_post_beta_variant_1": a_prior_beta_1,
            "a_post_beta_variant_2": a_prior_beta_2,
            "b_post_beta_variant_1": b_prior_beta_1,
            "b_post_beta_variant_2": b_prior_beta_2,
            "m_post_variant_1": m_prior_1,
            "m_post_variant_2": m_prior_2,
            "a_post_ig_variant_1": a_prior_ig_1,
            "a_post_ig_variant_2": a_prior_ig_2,
            "b_post_ig_variant_1": b_prior_ig_1,
            "b_post_ig_variant_2": b_prior_ig_2,
            "w_post_variant_1": w_prior_1,
            "w_post_variant_2": w_prior_2,
            "results": results_dict,
            "test_definition": test_definition_dict,
        }

        output_df = pd.Series(output_dict).to_frame().transpose()

        return output_df

    if datapoint_type == PossibleDatapointTypes.one_datapoint_per_user_per_meta_date:
        data_query = f"""
            WITH
                base_table AS (
                    SELECT user_id
                        , meta_date
                        , first_login
                        , CASE
                            WHEN group_tag = 'control'      THEN 'C'
                            WHEN group_tag = 'personalized' THEN 'P'
                        END                                                                              test_group
                        {spending_line}
                    FROM analytics__{company_id}__{project_id}.user_level_performance
                    WHERE meta_date  BETWEEN  DATE '{start_date}' AND  DATE '{end_date}'
                    AND first_login BETWEEN DATE '{min_first_login_date}' AND DATE '{max_first_login_date}'
                    GROUP BY user_id
                        , meta_date
                        , first_login
                        , group_tag
                    )

            SELECT MAX(meta_date) as meta_date
                , test_group
                , COUNT(*)                                         AS totals
                , SUM(CASE WHEN total_spend > 0 THEN 1 ELSE 0 END) AS positives
                , SUM(total_spend) AS sum_values
                , SUM(LN(CASE WHEN total_spend > 0 THEN total_spend END)) AS sum_logs
                , SUM(power(LN(CASE WHEN total_spend > 0 THEN total_spend END), 2.0)) AS sum_logs_squared
            FROM base_table
            GROUP BY test_group;"""
    elif (
        datapoint_type
        == PossibleDatapointTypes.one_datapoint_per_user_first_n_day_spend
    ):
        data_query = f"""
            WITH daily_user_spend AS (
                SELECT user_id
                    , meta_date
                    , first_login
                    , CASE
                        WHEN group_tag = 'control' THEN 'C'
                        WHEN group_tag = 'personalized' THEN 'P'
                    END                             test_group
                    {spending_line}
                FROM analytics__{company_id}__{project_id}.user_level_performance
                WHERE first_login BETWEEN  DATE '{start_date}' - INTERVAL '{spend_offset}' DAY AND  DATE '{end_date}' - INTERVAL '{spend_offset}' DAY
                AND first_login >= DATE '{min_first_login_date}'
                GROUP BY user_id
                    , meta_date
                    , first_login
                    , group_tag)

            , daily_user_spend_only_first_n_days AS (
                SELECT *
                FROM daily_user_spend
                WHERE meta_date >= first_login
                AND meta_date <= first_login + INTERVAL '{spend_offset}' DAY
                )

            , base_table AS (
                SELECT user_id, first_login as meta_date, MAX(test_group) as test_group, SUM(total_spend) AS total_spend
                FROM daily_user_spend_only_first_n_days
                GROUP BY user_id, first_login
                )
            
            
            SELECT MAX(meta_date) as meta_date
                , test_group
                , COUNT(*)                                              AS totals
                , SUM(CASE WHEN total_spend > 0 THEN 1 ELSE 0 END) AS positives
                , SUM(total_spend) AS sum_values
                , SUM(LN(CASE WHEN total_spend > 0 THEN total_spend END)) AS sum_logs
                , SUM(power(LN(CASE WHEN total_spend > 0 THEN total_spend END), 2.0)) AS sum_logs_squared
            FROM base_table
            GROUP BY test_group;"""

    data_df = FeatureStoreOfflineClient.run_athena_query_pandas(data_query)

    if data_df.empty:
        raise Exception("No data available for the provided inputs")

    data_variant_1 = data_df[(data_df.test_group == variant_name_1)].squeeze()
    data_variant_2 = data_df[(data_df.test_group == variant_name_2)].squeeze()

    # input params gotten through a query
    totals_1 = data_variant_1["totals"]
    totals_2 = data_variant_2["totals"]
    positives_1 = data_variant_1["positives"]
    positives_2 = data_variant_2["positives"]
    sum_values_1 = data_variant_1["sum_values"]
    sum_values_2 = data_variant_2["sum_values"]
    sum_logs_1 = data_variant_1["sum_logs"]
    sum_logs_2 = data_variant_2["sum_logs"]
    sum_logs_squared_1 = data_variant_1["sum_logs_squared"]
    sum_logs_squared_2 = data_variant_2["sum_logs_squared"]

    # testing
    test_revenue = DeltaLognormalDataTest()
    test_revenue.add_variant_data_agg(
        name=variant_name_1,
        totals=totals_1,
        positives=positives_1,
        sum_values=sum_values_1,
        sum_logs=sum_logs_1,
        sum_logs_2=sum_logs_squared_1,
        a_prior_beta=a_prior_beta_1,
        b_prior_beta=b_prior_beta_1,
        m_prior=m_prior_1,
        a_prior_ig=a_prior_ig_1,
        b_prior_ig=b_prior_ig_1,
        w_prior=w_prior_1,
    )
    test_revenue.add_variant_data_agg(
        name=variant_name_2,
        totals=totals_2,
        positives=positives_2,
        sum_values=sum_values_2,
        sum_logs=sum_logs_2,
        sum_logs_2=sum_logs_squared_2,
        a_prior_beta=a_prior_beta_2,
        b_prior_beta=b_prior_beta_2,
        m_prior=m_prior_2,
        a_prior_ig=a_prior_ig_2,
        b_prior_ig=b_prior_ig_2,
        w_prior=w_prior_2,
    )
    res_revenue_test = test_revenue.evaluate(seed=42)

    inputs_dict = {
        "totals_variant_1": totals_1,
        "totals_variant_2": totals_2,
        "positives_variant_1": positives_1,
        "positives_variant_2": positives_2,
        "sum_values_variant_1": sum_values_1,
        "sum_values_variant_2": sum_values_2,
        "sum_logs_variant_1": sum_logs_1,
        "sum_logs_variant_2": sum_logs_2,
        "sum_logs_squared_variant_1": sum_logs_squared_1,
        "sum_logs_squared_variant_2": sum_logs_squared_2,
        "a_prior_beta_variant_1": a_prior_beta_1,
        "a_prior_beta_variant_2": a_prior_beta_2,
        "b_prior_beta_variant_1": b_prior_beta_1,
        "b_prior_beta_variant_2": b_prior_beta_2,
        "m_prior_variant_1": m_prior_1,
        "m_prior_variant_2": m_prior_2,
        "a_prior_ig_variant_1": a_prior_ig_1,
        "a_prior_ig_variant_2": a_prior_ig_2,
        "b_prior_ig_variant_1": b_prior_ig_1,
        "b_prior_ig_variant_2": b_prior_ig_2,
        "w_prior_variant_1": w_prior_1,
        "w_prior_variant_2": w_prior_2,
    }

    results_dict = {
        "too_early_to_run_test": True,
        "prob_being_best_version_1": res_revenue_test[0]["prob_being_best"],
        "prob_being_best_version_2": res_revenue_test[1]["prob_being_best"],
        "expected_loss_version_1": res_revenue_test[0]["expected_loss"],
        "expected_loss_version_2": res_revenue_test[1]["expected_loss"],
        "expected_total_gain_version_1": res_revenue_test[0]["expected_total_gain"],
        "expected_total_gain_version_2": res_revenue_test[1]["expected_total_gain"],
    }

    output_dict = {
        "inputs": inputs_dict,
        "name_variant_1": variant_name_1,
        "name_variant_2": variant_name_2,
        "a_post_beta_variant_1": res_revenue_test[0]["a_post_beta"],
        "a_post_beta_variant_2": res_revenue_test[1]["a_post_beta"],
        "b_post_beta_variant_1": res_revenue_test[0]["b_post_beta"],
        "b_post_beta_variant_2": res_revenue_test[1]["b_post_beta"],
        "m_post_variant_1": res_revenue_test[0]["m_post"],
        "m_post_variant_2": res_revenue_test[1]["m_post"],
        "a_post_ig_variant_1": res_revenue_test[0]["a_post_ig"],
        "a_post_ig_variant_2": res_revenue_test[1]["a_post_ig"],
        "b_post_ig_variant_1": res_revenue_test[0]["b_post_ig"],
        "b_post_ig_variant_2": res_revenue_test[1]["b_post_ig"],
        "w_post_variant_1": res_revenue_test[0]["w_post"],
        "w_post_variant_2": res_revenue_test[1]["w_post"],
        "results": results_dict,
        "test_definition": test_definition_dict,
    }

    output_df = pd.Series(output_dict).to_frame().transpose()

    return output_df


if __name__ == "__main__":
    company_id = os.environ["COMPANY_ID"]
    project_id = os.environ["PROJECT_ID"]
    test_name = os.environ["NAME"]
    ab_test_id = os.environ["AB_TEST_ID"]
    start_date = os.environ["TEST_START_DATE"]
    end_date = os.environ["TEST_END_DATE"]
    winsorized = bool(os.environ["WINSORIZED_SPEND"])
    personalized = bool(os.environ["PERSONALIZED_SPEND"])
    datapoint_type = os.environ["DATAPOINT_TYPE"]
    n_days_spend = int(os.environ["N_DAYS_SPEND"])
    min_first_login_date = os.environ["MIN_FIRST_LOGIN_DATE"]
    max_first_login_date = os.environ["MAX_FIRST_LOGIN_DATE"]
    variant_name_1 = os.environ["VARIANT1_NAME"]
    variant_name_2 = os.environ["VARIANT2_NAME"]
    a_prior_beta_1 = float(os.environ["VARIANT1_PRIOR_A_BETA"])
    a_prior_beta_2 = float(os.environ["VARIANT1_PRIOR_A_BETA"])
    b_prior_beta_1 = float(os.environ["VARIANT1_PRIOR_B_BETA"])
    b_prior_beta_2 = float(os.environ["VARIANT1_PRIOR_B_BETA"])
    m_prior_1 = float(os.environ["VARIANT1_PRIOR_MU"])
    m_prior_2 = float(os.environ["VARIANT2_PRIOR_MU"])
    a_prior_ig_1 = float(os.environ["VARIANT1_PRIOR_A_IG"])
    a_prior_ig_2 = float(os.environ["VARIANT2_PRIOR_A_IG"])
    b_prior_ig_1 = float(os.environ["VARIANT1_PRIOR_B_IG"])
    b_prior_ig_2 = float(os.environ["VARIANT2_PRIOR_B_IG"])
    w_prior_1 = float(os.environ["VARIANT1_PRIOR_W"])
    w_prior_2 = float(os.environ["VARIANT2_PRIOR_W"])
    initial_test_start_date = os.environ["INITIAL_TEST_START_DATE"]
    output_location = os.environ["OUTPUT_LOCATION"]
    output_file_name = os.environ["OUTPUT_FILE_NAME"]

    ab_testing_output = run_ab_testing(
        start_date=start_date,
        end_date=end_date,
        company_id=PossibleCompanyIds(company_id),
        project_id=PossibleProjectIds(project_id),
        test_name=test_name,
        ab_test_id=ab_test_id,
        personalized=personalized,
        winsorized=winsorized,
        datapoint_type=PossibleDatapointTypes(datapoint_type),
        min_first_login_date=min_first_login_date,
        max_first_login_date=max_first_login_date,
        n_days_spend=n_days_spend,
        variant_name_1=variant_name_1,
        variant_name_2=variant_name_2,
        a_prior_beta_1=a_prior_beta_1,
        a_prior_beta_2=a_prior_beta_2,
        b_prior_beta_1=b_prior_beta_1,
        b_prior_beta_2=b_prior_beta_2,
        m_prior_1=m_prior_1,
        m_prior_2=m_prior_2,
        a_prior_ig_1=a_prior_ig_1,
        a_prior_ig_2=a_prior_ig_2,
        b_prior_ig_1=b_prior_ig_1,
        b_prior_ig_2=b_prior_ig_2,
        w_prior_1=w_prior_1,
        w_prior_2=w_prior_2,
        initial_test_start_date=initial_test_start_date,
    )

    ab_testing_output.to_parquet("temp_ab_testing_output.parquet")

    s3 = boto3.client("s3")
    s3.upload_file(
        "temp_ab_testing_output.parquet",
        output_location,
        output_file_name,
    )
    os.remove("temp_ab_testing_output.parquet")
