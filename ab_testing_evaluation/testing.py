from enum import Enum
from datetime import datetime, timedelta

import boto3
import pandas as pd
from pydantic import Extra, BaseSettings
from ml_lib.feature_store.offline.client import FeatureStoreOfflineClient

from bayesian_testing.experiments import DeltaLognormalDataTest


class PossibleCompanyIds(str, Enum):
    century_games = "century-games-ncmgu"
    tinysoft = "tinysoft-a9kwp"
    tilting_point = "tilting-point-mjs4k"
    sparkgaming = "sparkgaming-vjv6s"


class PossibleProjectIds(str, Enum):
    spongebob = "spongebob-x7d9q"
    terragenesis = "terragenesis-m89uz"
    idle_mafia = "idle-mafia-ecbqb"
    bingo_aloha = "bingo-aloha-r3g9v"
    ultimatex = "ultimate-x-poker-rib6t"
    heroes_magic_war = "heroes-magic-war-h2sln"


class PossibleDatapointTypes(str, Enum):
    one_datapoint_per_user_per_meta_date = "one_datapoint_per_user_per_meta_date"
    one_datapoint_per_user_first_n_day_spend = (
        "one_datapoint_per_user_first_n_day_spend"
    )


class AbTestEvaluationConfig(
    BaseSettings,
    extra=Extra.forbid,
    env_prefix="AB_TESTING_EVALUATION_PARAM_",
    case_sensitive=False,
):
    company_id: PossibleCompanyIds
    project_id: PossibleProjectIds
    test_name: str
    ab_test_id: str
    start_date: str
    end_date: str
    winsorized: bool
    personalized: bool
    datapoint_type: PossibleDatapointTypes
    n_days_spend: int
    min_first_login_date: str
    max_first_login_date: str
    variant_name_1: str
    variant_name_2: str
    a_prior_beta_1: float
    a_prior_beta_2: float
    b_prior_beta_1: float
    b_prior_beta_2: float
    m_prior_1: float
    m_prior_2: float
    a_prior_ig_1: float
    a_prior_ig_2: float
    b_prior_ig_1: float
    b_prior_ig_2: float
    w_prior_1: float
    w_prior_2: float
    initial_test_start_date: str
    output_bucket: str
    output_key: str


def run_ab_testing(config: AbTestEvaluationConfig) -> pd.DataFrame:
    # create dict for saving the test definition
    test_definition_dict = {
        "company_id": config.company_id.value,
        "project_id": config.project_id.value,
        "test_name": config.test_name,
        "ab_test_id": config.ab_test_id,
        "intitial_test_start_date": config.initial_test_start_date,
        "personalized": config.personalized,
        "winsorized": config.winsorized,
        "datapoint_type": config.datapoint_type.value,
        "min_first_login_date": config.min_first_login_date,
        "max_first_login_date": config.max_first_login_date,
    }

    sanitized_company_id = config.company_id.replace("-", "_")
    sanitized_project_id = config.project_id.replace("-", "_")

    # prepare query params
    if config.personalized:
        personalized_num: int = 0
    else:
        personalized_num = 9

    if config.winsorized:
        spend_column: str = "spend"
    else:
        spend_column = "wins_spend"

    if config.project_id in ["spongebob-x7d9q", "terragenesis-m89uz"]:
        spending_line = f", SUM({spend_column}) as total_spend"
    else:
        spending_line = f", COALESCE(SUM(CASE WHEN fl_personalized_offer_spend <> {personalized_num} THEN {spend_column} END), 0) total_spend"

    if config.n_days_spend:
        spend_offset = str(config.n_days_spend - 1)

    # check whether it isn't too early to perform the test
    too_early_to_run_test = False
    end_date_datetime = datetime.strptime(config.end_date, "%Y-%m-%d")
    initial_test_start_date_datetime = datetime.strptime(
        config.initial_test_start_date, "%Y-%m-%d"
    )
    if (
        config.datapoint_type
        == PossibleDatapointTypes.one_datapoint_per_user_per_meta_date
    ):
        if end_date_datetime < initial_test_start_date_datetime:
            too_early_to_run_test = True
    elif (
        config.datapoint_type
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
            "name_variant_1": config.variant_name_1,
            "name_variant_2": config.variant_name_2,
            "a_post_beta_variant_1": config.a_prior_beta_1,
            "a_post_beta_variant_2": config.a_prior_beta_2,
            "b_post_beta_variant_1": config.b_prior_beta_1,
            "b_post_beta_variant_2": config.b_prior_beta_2,
            "m_post_variant_1": config.m_prior_1,
            "m_post_variant_2": config.m_prior_2,
            "a_post_ig_variant_1": config.a_prior_ig_1,
            "a_post_ig_variant_2": config.a_prior_ig_2,
            "b_post_ig_variant_1": config.b_prior_ig_1,
            "b_post_ig_variant_2": config.b_prior_ig_2,
            "w_post_variant_1": config.w_prior_1,
            "w_post_variant_2": config.w_prior_2,
            "results": results_dict,
            "test_definition": test_definition_dict,
        }

        output_df = pd.Series(output_dict).to_frame().transpose()

        return output_df

    if (
        config.datapoint_type
        == PossibleDatapointTypes.one_datapoint_per_user_per_meta_date
    ):
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
                    FROM analytics__{sanitized_company_id}__{sanitized_project_id}.user_level_performance
                    WHERE meta_date  BETWEEN  DATE '{config.start_date}' AND  DATE '{config.end_date}'
                    AND first_login BETWEEN DATE '{config.min_first_login_date}' AND DATE '{config.max_first_login_date}'
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
        config.datapoint_type
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
                FROM analytics__{sanitized_company_id}__{sanitized_project_id}.user_level_performance
                WHERE first_login BETWEEN  DATE '{config.start_date}' - INTERVAL '{spend_offset}' DAY AND  DATE '{config.end_date}' - INTERVAL '{spend_offset}' DAY
                AND first_login >= DATE '{config.min_first_login_date}'
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

    data_variant_1 = data_df[(data_df.test_group == config.variant_name_1)].squeeze()
    data_variant_2 = data_df[(data_df.test_group == config.variant_name_2)].squeeze()

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
        name=config.variant_name_1,
        totals=totals_1,
        positives=positives_1,
        sum_values=sum_values_1,
        sum_logs=sum_logs_1,
        sum_logs_2=sum_logs_squared_1,
        a_prior_beta=config.a_prior_beta_1,
        b_prior_beta=config.b_prior_beta_1,
        m_prior=config.m_prior_1,
        a_prior_ig=config.a_prior_ig_1,
        b_prior_ig=config.b_prior_ig_1,
        w_prior=config.w_prior_1,
    )
    test_revenue.add_variant_data_agg(
        name=config.variant_name_2,
        totals=totals_2,
        positives=positives_2,
        sum_values=sum_values_2,
        sum_logs=sum_logs_2,
        sum_logs_2=sum_logs_squared_2,
        a_prior_beta=config.a_prior_beta_2,
        b_prior_beta=config.b_prior_beta_2,
        m_prior=config.m_prior_2,
        a_prior_ig=config.a_prior_ig_2,
        b_prior_ig=config.b_prior_ig_2,
        w_prior=config.w_prior_2,
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
        "a_prior_beta_variant_1": config.a_prior_beta_1,
        "a_prior_beta_variant_2": config.a_prior_beta_2,
        "b_prior_beta_variant_1": config.b_prior_beta_1,
        "b_prior_beta_variant_2": config.b_prior_beta_2,
        "m_prior_variant_1": config.m_prior_1,
        "m_prior_variant_2": config.m_prior_2,
        "a_prior_ig_variant_1": config.a_prior_ig_1,
        "a_prior_ig_variant_2": config.a_prior_ig_2,
        "b_prior_ig_variant_1": config.b_prior_ig_1,
        "b_prior_ig_variant_2": config.b_prior_ig_2,
        "w_prior_variant_1": config.w_prior_1,
        "w_prior_variant_2": config.w_prior_2,
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
        "name_variant_1": config.variant_name_1,
        "name_variant_2": config.variant_name_2,
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


def upload_output_to_s3(output: pd.DataFrame, bucket: str, key: str):
    s3 = boto3.client("s3")

    output_json = output.to_json(None, orient="records", lines=True)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=output_json,
    )
