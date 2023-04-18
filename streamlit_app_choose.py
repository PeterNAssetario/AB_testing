from pathlib import Path

import arviz as az
import numpy as np
import altair as alt
import pandas as pd
import seaborn as sns
import streamlit as st
import matplotlib as plt
import scipy.stats
from datetime import datetime, timedelta
from scipy.stats import norm
from ml_lib.feature_store import configure_offline_feature_store
from ml_lib.feature_store.offline.client import FeatureStoreOfflineClient

from ab_testing.constants import target_col, client_name
from ab_testing.data_acquisition.acquire_data import queries_dict  # AcquireData
from ab_testing.predictions.produce_predictions import ProducePredictions, run_ab_testing
from ab_testing.distribution_fit.fit_distribution import FitDistribution

allow_reuse = True

st.set_page_config(
    page_title="AB Testing",
    page_icon="📊",
    initial_sidebar_state="expanded",
    layout="wide",
)

st.write(
    """
# 📊 A/B Testing
Choose client to see the performance of their A/B test.
"""
)

option = st.selectbox(
    "",
    (
        "Idle Mafia",
        "Spongebob",
        "Bingo Aloha",
        "Terra Genesis",
        "Ultimex",
        "Heroes Magic War",
    ),
    help="Which client would you like to view?",
    index=0,
)
client_map = {
    "Bingo Aloha": "bingo_aloha",
    "Heroes Magic War": "homw",
    "Idle Mafia": "idle_mafia",
    "Spongebob": "spongebob",
    "Terra Genesis": "terra_genesis",
    "Ultimex": "ultimex",
}

client_name = client_map[option]
ab_default = "test_group"
result_default = "total_wins_spend"
spend_default = "personalised"

#configure_offline_feature_store(workgroup="development", catalog_name="production")
configure_offline_feature_store(workgroup="primary")

if client_name:
    initial_data = FeatureStoreOfflineClient.run_athena_query_pandas(
        queries_dict[client_name + "_sample"]
    )
    data_date_limits = FeatureStoreOfflineClient.run_athena_query_pandas(
        queries_dict[client_name + "_date_limits"]
    )

    st.markdown("### Data Preview")
    st.dataframe(initial_data.head())

    st.markdown("### Select Analysis Options")
    with st.form(key="my_form"):
        set1_col1, set1_col2, set1_col3 = st.columns(3)
        with set1_col1:
            ab = st.multiselect(
                "A/B Column",
                options=initial_data.columns,
                help="Select which column refers to A/B test labels.",
                default=ab_default,
            )
        with set1_col2:
            result = st.multiselect(
                "Result Column",
                options=initial_data.columns,
                help="Select which column shows results of A/B test.",
                default=result_default,
            )
        if client_name not in ["spongebob", "terra_genesis"]:
            with set1_col3:
                spend_type = st.multiselect(
                    "Spend Type",
                    options=["personalised", "non-personalised"],
                    help="Select what type of spend you wish to analyse, select both for both.",
                    default=spend_default,
                )
        else:
            spend_type = []

        min_date_val = data_date_limits.min_meta_date[0]
        max_date_val = data_date_limits.max_meta_date[0]
        set2_col1, set2_col2 = st.columns(2)
        with set2_col1:
            start_date = st.date_input(
                "Select Start Date",
                max_date_val - timedelta(days=30),
                min_value=min_date_val,
                max_value=max_date_val,
            )
        with set2_col2:
            end_date = st.date_input(
                "Select End Date",
                max_date_val,
                min_value=min_date_val,
                max_value=max_date_val,
            )

        min_fl_val = data_date_limits.min_first_login[0]
        max_fl_val = data_date_limits.max_first_login[0]
        set3_col1, set3_col2 = st.columns(2)
        with set3_col1:
            start_fl = st.date_input(
                "Select Start First Login",
                min_fl_val,
                min_value=min_fl_val,
                max_value=max_fl_val,
            )
        with set3_col2:
            end_fl = st.date_input(
                "Select End First Login",
                max_fl_val,
                min_value=min_fl_val,
                max_value=max_fl_val,
            )

        # Change this, currently not used:
        # if ab:
        #    control = initial_data[ab[0]].unique()[0]
        #    treatment = initial_data[ab[0]].unique()[1]
        #    decide = st.radio(
        #        f"Is *{treatment}* Group B?",
        #        options=["Yes", "No"],
        #        help=f"Select yes if this is group B (or the treatment group) from your test.\nThus, {control} is Group A (control).",
        #    )
        #    if decide == "No":
        #        control, treatment = treatment, control

        with st.expander("Adjust test parameters"):
            st.markdown("### Parameters")
            st.slider(
                "Posterior Creadibility (HDI)",
                min_value=0.80,
                max_value=0.99,
                value=0.90,
                step=0.01,
                key="hdi",
                help=" Values of θ that have at least some minimal level of posterior credibility, such that the total probability of all such θ values is HDI% ",
            )

        submit_button = st.form_submit_button(label="Submit")

    if submit_button:
        if not ab or not result:
            st.warning("Please select both an **A/B column** and a **Result column**.")
            st.stop()

        if len(spend_type) != 1:
            spend_type = 9
        elif spend_type[0] == "personalised":
            spend_type = 0
        elif spend_type[0] == "non-personalised":
            spend_type = 1
        initial_data = FeatureStoreOfflineClient.run_athena_query_pandas(
            queries_dict[client_name],
            {
                "strt_date": str(start_date)[0:10],
                "end_date": str(end_date)[0:10],
                "strt_fl": str(start_fl)[0:10],
                "end_fl": str(end_fl)[0:10],
                "spend_type": int(spend_type),
            },
        )

        row0_space1, row0_col1, row0_space2, row0_col2, row0_space3 = st.columns(
            (0.05, 1, 0.05, 1, 0.05)
        )
        with row0_col1:
            st.write("")
            st.write("## AB Test Performance For:\n###", option)
            st.write("")
        with row0_col2:
            fit_dist = FitDistribution(fname=f"{client_name}_distribution_fit.p")
            best_distribution = fit_dist.fit(
                initial_data.loc[initial_data[result[0]] > 0], result[0]
            )

            st.write("")
            st.write("## Distribution Used (Best Fit):\n###", best_distribution)
            st.write("")

        # Create test results:
        result = ProducePredictions()
        results_conversion = result.produce_results_conversion(initial_data)
        results_revenue = result.produce_results_revenue(
            best_distribution, initial_data
        )
        results_posterior_sample = result._produce_results_lognorm_dist_carry_value(
            initial_data
        )

        # Set up metrics:
        post_sample_A = results_posterior_sample[1]
        post_sample_B = results_posterior_sample[0]
        post_sample_uplift = (post_sample_B - post_sample_A) / post_sample_A
        post_sample_diff = post_sample_B - post_sample_A
        hdi_A = az.hdi(post_sample_A, hdi_prob=st.session_state.hdi)
        hdi_B = az.hdi(post_sample_B, hdi_prob=st.session_state.hdi)
        hdi_diff = az.hdi(post_sample_uplift, hdi_prob=st.session_state.hdi)
        hdi_diff_ab = az.hdi(post_sample_diff, hdi_prob=st.session_state.hdi)

        # Draw up tables:
        st.write("")
        row1_space1, row1_col1, row1_space2, row1_col2, row1_space3 = st.columns(
            (0.1, 1, 0.1, 1, 0.1)
        )
        with row1_col1:
            st.metric(
                "Delta ARPUs",
                value="%.4f$"
                % (results_revenue[0]["avg_values"] - results_revenue[1]["avg_values"]),
            )
        with row1_col2:
            st.metric(
                "Delta Conversion",
                value="%.2f%%"
                % (
                    (
                        results_conversion[0]["positive_rate"]
                        - results_conversion[1]["positive_rate"]
                    )
                    * 100
                ),
            )

        st.write("")
        plt.use("agg")
        _lock = plt.backends.backend_agg.RendererAgg.lock
        sns.set_style("darkgrid")

        # Set up plots:
        row2_space1, row2_col1, row2_space2, row2_col2, row2_space3 = st.columns(
            (0.05, 1, 0.05, 1, 0.05)
        )

        with row2_col1, _lock:
            st.subheader("Distribution of posterior ARPU A & B")
            fig = plt.figure.Figure()
            ax = fig.add_subplot(111)
            fig_temp = sns.kdeplot(post_sample_A, color="blue", ax=ax)
            fig_temp = sns.kdeplot(post_sample_B, color="red", ax=ax)
            l1 = fig_temp.lines[0]
            l2 = fig_temp.lines[1]
            x1 = l1.get_xydata()[:, 0]
            x2 = l2.get_xydata()[:, 0]
            y1 = l1.get_xydata()[:, 1]
            y2 = l2.get_xydata()[:, 1]
            x1_new = x1[
                [all(tup) for tup in zip(list(x1 >= hdi_A[0]), list(x1 <= hdi_A[1]))]
            ]
            x2_new = x2[
                [all(tup) for tup in zip(list(x2 >= hdi_B[0]), list(x2 <= hdi_B[1]))]
            ]
            y1_new = y1[
                [all(tup) for tup in zip(list(x1 >= hdi_A[0]), list(x1 <= hdi_A[1]))]
            ]
            y2_new = y2[
                [all(tup) for tup in zip(list(x2 >= hdi_B[0]), list(x2 <= hdi_B[1]))]
            ]
            ax.fill_between(x1_new, y1_new, color="blue", alpha=0.3)
            ax.fill_between(x2_new, y2_new, color="red", alpha=0.3)
            ax.legend(labels=["Control", "Personalised"])
            st.pyplot(fig)

        with row2_col2, _lock:
            st.subheader("Approximate Distribution of Uplifts")
            fig2 = plt.figure.Figure()
            ax2 = fig2.add_subplot(111)
            fig_temp2 = sns.kdeplot(post_sample_uplift, color="purple", ax=ax2)
            l = fig_temp2.lines[0]
            x = l.get_xydata()[:, 0]
            y = l.get_xydata()[:, 1]
            x_new = x[
                [
                    all(tup)
                    for tup in zip(list(x >= hdi_diff[0]), list(x <= hdi_diff[1]))
                ]
            ]
            y_new = y[
                [
                    all(tup)
                    for tup in zip(list(x >= hdi_diff[0]), list(x <= hdi_diff[1]))
                ]
            ]
            ax2.xaxis.set_major_formatter(
                plt.ticker.PercentFormatter(xmax=1, decimals=2)
            )
            ax2.fill_between(x_new, y_new, color="purple", alpha=0.3)
            st.pyplot(fig2)

        # Set up end tables:
        row3_space1, row3_col1, row3_space2, row3_col2, row3_space3 = st.columns(
            (0.05, 1, 0.05, 1, 0.05)
        )

        # Table1
        output_df = pd.DataFrame(columns=["Metric", "Conversion", "Revenue"])
        output_df["Metric"] = ["P(P > C)", "E(loss | P > C)", "E(loss | C > P)"]
        output_df["Conversion"] = [
            "%.2f%%" % (results_conversion[0]["prob_being_best"] * 100),
            "%.2f%%" % (results_conversion[0]["expected_loss"] * 100),
            "%.2f%%" % (results_conversion[1]["expected_loss"] * 100),
        ]
        output_df["Revenue"] = [
            "%.2f%%" % (results_revenue[0]["prob_being_best"] * 100),
            "%.4f€" % (results_revenue[0]["expected_loss"]),
            "%.4f€" % (results_revenue[1]["expected_loss"]),
        ]
        output_df = output_df.set_index("Metric")
        table1 = row3_col2.write(output_df)

        # Table2
        output_df2 = pd.DataFrame(
            columns=["Metric", "Control", "Personalised", "Personalised-Control"]
        )
        output_df2["Metric"] = ["sample size", "conversion", "ARPU", "ARPPU", "95% HDI"]
        output_df2["Control"] = [
            "%d" % (results_revenue[1]["totals"]),
            "%.2f%%" % (results_conversion[1]["positive_rate"] * 100),
            "%.4f$" % (results_revenue[1]["avg_values"]),
            "%.4f$" % (results_revenue[1]["avg_positive_values"]),
            "[%.4f$, %.4f$]" % (hdi_A[0], hdi_A[1]),
        ]
        output_df2["Personalised"] = [
            "%d" % (results_revenue[0]["totals"]),
            "%.2f%%" % (results_conversion[0]["positive_rate"] * 100),
            "%.4f$" % (results_revenue[0]["avg_values"]),
            "%.4f$" % (results_revenue[0]["avg_positive_values"]),
            "[%.4f$, %.4f$]" % (hdi_B[0], hdi_B[1]),
        ]
        output_df2["Personalised-Control"] = [
            np.NAN,
            "%.2f%%"
            % (
                (
                    results_conversion[0]["positive_rate"]
                    - results_conversion[1]["positive_rate"]
                )
                * 100
            ),
            "%.4f$"
            % (results_revenue[0]["avg_values"] - results_revenue[1]["avg_values"]),
            "%.4f$"
            % (
                results_revenue[0]["avg_positive_values"]
                - results_revenue[1]["avg_positive_values"]
            ),
            "[%.4f$, %.4f$]" % (hdi_diff_ab[0], hdi_diff_ab[1]),
        ]
        output_df2 = output_df2.set_index("Metric")
        table2 = row3_col1.write(output_df2)
        
        st.write("## Daily Test Progression:")
        dates_list = [(start_date + timedelta(days=x)) for x in range((end_date-start_date).days + 1)]
        print(dates_list)
        run_ab_testing(initial_data, dates_list)