from typing import Dict, List, Union

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# https://github.com/Matt52/bayesian-testing
from bayesian_testing.experiments import BinaryDataTest, DeltaLognormalDataTest

from ab_testing.constants import target_col


class ProducePredictions:
    def __init__(self, seed: int = 42):

        self.seed = seed

    def produce_results_revenue(
        self, dist_name: str, df: pd.DataFrame
    ) -> List[Dict[str, Union[float, int]]]:

        if dist_name == "lognorm":
            return self._produce_results_lognorm_dist(df)
        else:
            raise NotImplementedError(
                "Results for given distribution are not implemented yet."
            )

    def produce_results_conversion(
        self, df: pd.DataFrame
    ) -> List[Dict[str, Union[float, int]]]:
        dfc = df.copy()

        dfc["conversions"] = np.where(dfc[target_col] > 0, 1, 0)

        df_P = dfc.loc[
            (dfc["test_group"].str.lower() == "p")
            | (dfc["test_group"].str.lower() == "assetario")
        ]
        df_C = dfc.loc[
            (dfc["test_group"].str.lower() == "c")
            | (dfc["test_group"].str.lower() == "control")
        ]

        test = BinaryDataTest()

        test.add_variant_data("P", df_P["conversions"].values)
        test.add_variant_data("C", df_C["conversions"].values)

        return test.evaluate(seed=self.seed)

    def _produce_results_lognorm_dist(
        self, df: pd.DataFrame
    ) -> List[Dict[str, Union[float, int]]]:

        df_P = df.loc[
            (df["test_group"].str.lower() == "p")
            | (df["test_group"].str.lower() == "assetario")
        ]
        df_C = df.loc[
            (df["test_group"].str.lower() == "c")
            | (df["test_group"].str.lower() == "control")
        ]

        test = DeltaLognormalDataTest()

        test.add_variant_data("P", df_P["total_wins_spend"].values)
        test.add_variant_data("C", df_C["total_wins_spend"].values)
        
        return test.evaluate(seed=42)

    def _produce_results_lognorm_dist_carry_value(self, df: pd.DataFrame) -> list:
        df_P = df.loc[
            (df["test_group"].str.lower() == "p")
            | (df["test_group"].str.lower() == "assetario")
        ]
        df_C = df.loc[
            (df["test_group"].str.lower() == "c")
            | (df["test_group"].str.lower() == "control")
        ]

        test = DeltaLognormalDataTest()

        test.add_variant_data("P", df_P["total_wins_spend"].values)
        test.add_variant_data("C", df_C["total_wins_spend"].values)

        return test.carry_value(seed=42)

def run_ab_testing(data_df, dates_list):
    list_probabs_conv_test = []
    list_losses_conv_test_P = []
    list_losses_conv_test_C = []
    list_total_gains_conv_test_P = []
    list_total_gains_conv_test_C = []
    list_conversion_rates_P = []
    list_conversion_rates_C = []

    num_datapoints_P = 0
    num_datapoints_C = 0
    num_conversions_P = 0
    num_conversions_C = 0

    a_prior_conv_test_C = 0.5
    b_prior_conv_test_C = 0.5
    a_prior_conv_test_P = 0.5
    b_prior_conv_test_P = 0.5
    

    list_probabs_revenue_test = []
    list_losses_revenue_test_P = []
    list_losses_revenue_test_C = []
    list_total_gains_revenue_test_P = []
    list_total_gains_revenue_test_C = []
    list_average_spends_P = []
    list_average_spends_C = []

    sum_spends_P = 0
    sum_spends_C = 0

    a_prior_beta_revenue_test_P = 0.5
    b_prior_beta_revenue_test_P = 0.5
    m_prior_revenue_test_P = 1
    a_prior_ig_revenue_test_P = 0
    b_prior_ig_revenue_test_P = 0
    w_prior_revenue_test_P = 0.01

    a_prior_beta_revenue_test_C = 0.5
    b_prior_beta_revenue_test_C = 0.5
    m_prior_revenue_test_C = 1
    a_prior_ig_revenue_test_C = 0
    b_prior_ig_revenue_test_C = 0
    w_prior_revenue_test_C = 0.01


    for current_date in dates_list:
        try:
            daily_data = data_df[data_df.meta_date == current_date].copy()
            daily_data['conversions'] = daily_data['total_wins_spend'] > 0
            df_P = daily_data.loc[(daily_data["test_group"].str.lower() == "p")
                    | (daily_data["test_group"].str.lower() == "assetario")]
            df_C = daily_data.loc[(daily_data["test_group"].str.lower() == "c")
                    | (daily_data["test_group"].str.lower() == "control")]

            #################################################################
            # CONVERSION RATE TEST
            test_conversion = BinaryDataTest()

            test_conversion.add_variant_data(name = "P", data = df_P["conversions"].values, a_prior = a_prior_conv_test_P, b_prior = b_prior_conv_test_P)
            test_conversion.add_variant_data(name = "C", data = df_C["conversions"].values, a_prior = a_prior_conv_test_C, b_prior = b_prior_conv_test_C)

            a_prior_conv_test_P = test_conversion.data['P']['a_posterior']
            b_prior_conv_test_P = test_conversion.data['P']['b_posterior']
            a_prior_conv_test_C = test_conversion.data['C']['a_posterior']
            b_prior_conv_test_C = test_conversion.data['C']['b_posterior']

            res_conv_test = test_conversion.evaluate(seed=42)
            list_probabs_conv_test.append(res_conv_test[0]['prob_being_best'])
            list_losses_conv_test_P.append(res_conv_test[0]['expected_loss'])
            list_losses_conv_test_C.append(res_conv_test[1]['expected_loss'])
            list_total_gains_conv_test_P.append(res_conv_test[0]['expected_total_gain'])
            list_total_gains_conv_test_C.append(res_conv_test[1]['expected_total_gain'])

            num_datapoints_P += df_P.shape[0]
            num_conversions_P += df_P[df_P.total_wins_spend > 0].shape[0]
            num_datapoints_C += df_C.shape[0]
            num_conversions_C += df_C[df_C.total_wins_spend > 0].shape[0]

            list_conversion_rates_P.append(num_conversions_P/num_datapoints_P)
            list_conversion_rates_C.append(num_conversions_C/num_datapoints_C)
            #################################################################

            #################################################################
            # REVENUE TEST
            test_revenue = DeltaLognormalDataTest()
            test_revenue.add_variant_data(name = "P", data = df_P["total_wins_spend"].values, a_prior_beta = a_prior_beta_revenue_test_P, b_prior_beta = b_prior_beta_revenue_test_P, 
                                            m_prior = m_prior_revenue_test_P, a_prior_ig = a_prior_ig_revenue_test_P, b_prior_ig = b_prior_ig_revenue_test_P, w_prior = w_prior_revenue_test_P)
            test_revenue.add_variant_data(name = "C", data = df_C["total_wins_spend"].values, a_prior_beta = a_prior_beta_revenue_test_C, b_prior_beta = b_prior_beta_revenue_test_C, 
                                            m_prior = m_prior_revenue_test_C, a_prior_ig = a_prior_ig_revenue_test_C, b_prior_ig = b_prior_ig_revenue_test_C, w_prior = w_prior_revenue_test_C)

            a_prior_beta_revenue_test_P = test_revenue.data['P']['a_post_beta']
            b_prior_beta_revenue_test_P = test_revenue.data['P']['b_post_beta']
            m_prior_revenue_test_P = test_revenue.data['P']['m_post']
            a_prior_ig_revenue_test_P = test_revenue.data['P']['a_post_ig']
            b_prior_ig_revenue_test_P = test_revenue.data['P']['b_post_ig']
            w_prior_revenue_test_P = test_revenue.data['P']['w_post']

            a_prior_beta_revenue_test_C = test_revenue.data['C']['a_post_beta']
            b_prior_beta_revenue_test_C = test_revenue.data['C']['b_post_beta']
            m_prior_revenue_test_C = test_revenue.data['C']['m_post']
            a_prior_ig_revenue_test_C = test_revenue.data['C']['a_post_ig']
            b_prior_ig_revenue_test_C = test_revenue.data['C']['b_post_ig']
            w_prior_revenue_test_C = test_revenue.data['C']['w_post']

            res_revenue_test = test_revenue.evaluate(seed=42)
            list_probabs_revenue_test.append(res_revenue_test[0]['prob_being_best'])
            list_losses_revenue_test_P.append(res_revenue_test[0]['expected_loss'])
            list_losses_revenue_test_C.append(res_revenue_test[1]['expected_loss'])
            list_total_gains_revenue_test_P.append(res_conv_test[0]['expected_total_gain'])
            list_total_gains_revenue_test_C.append(res_conv_test[1]['expected_total_gain'])
        
            sum_spends_P +=  df_P.total_wins_spend.sum()
            sum_spends_C +=  df_C.total_wins_spend.sum()

            list_average_spends_P.append(sum_spends_P/num_datapoints_P)
            list_average_spends_C.append(sum_spends_C/num_datapoints_C)
            #################################################################
        except:
            list_probabs_conv_test.append(None)
            list_losses_conv_test_P.append(None)
            list_losses_conv_test_C.append(None)
            list_total_gains_conv_test_P.append(None)
            list_total_gains_conv_test_C.append(None)
            list_conversion_rates_P.append(None)
            list_conversion_rates_C.append(None)
            
            list_probabs_revenue_test.append(None)
            list_losses_revenue_test_P.append(None)
            list_losses_revenue_test_C.append(None)
            list_total_gains_revenue_test_P.append(None)
            list_total_gains_revenue_test_C.append(None)
            list_average_spends_P.append(None)
            list_average_spends_C.append(None)
    
    return([list_probabs_revenue_test,
            list_losses_revenue_test_P,
            list_losses_revenue_test_C,
            list_total_gains_revenue_test_P,
            list_total_gains_revenue_test_C,
            list_average_spends_P,
            list_average_spends_C,
           ])