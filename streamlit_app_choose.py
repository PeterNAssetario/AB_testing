import streamlit as st
import pandas as pd
import numpy as np
import scipy.stats
from scipy.stats import norm
import altair as alt

import matplotlib as plt
import seaborn as sns
import arviz as az
from pathlib import Path

from ab_testing.constants import client_name, target_col
from ab_testing.distribution_fit.fit_distribution import FitDistribution
from ab_testing.predictions.produce_predictions import ProducePredictions
from ab_testing.data_acquisition.acquire_data import queries_dict #AcquireData

from ml_lib.feature_store import configure_offline_feature_store
from ml_lib.feature_store.offline.client import FeatureStoreOfflineClient

st.set_page_config(
    page_title="AB Testing", page_icon="📊", initial_sidebar_state="expanded", layout="wide"
)

st.write(
    """
# 📊 A/B Testing
Choose client to see the performance of their A/B test.
"""
)

option = st.selectbox(
    '',
    ('Demo', 'Idle Mafia', 'Bingo Aloha', 'Spongebob', 'Terra Genesis', 'Ultimex', 'Knighthood', 'Heroes of Magic and War'),
    help = 'Which client would you like to view?',
    index = 0
)
client_map = {
    'Bingo Aloha':"bingo_aloha",
    'Heroes of Magic and War':"homw",
    'Idle Mafia':"idle_mafia",
    'Knighthood':"knighthood",
    'Spongebob':"spongebob",
    'Terra Genesis':"terra_genesis",
    'Ultimex':"ultimex",
}
if option == 'Demo':
    client_name = option
    ab_default = "test_group"
    result_default = "total_wins_spend"  
else:
    client_name = client_map[option]
    ab_default = None
    result_default = None

if client_name:
    if client_name == 'Demo':
        initial_data = pd.read_parquet('./test_data.p')
    else:
        try: # I have diff. work group then yall...
            configure_offline_feature_store(workgroup = 'primary')
            initial_data = FeatureStoreOfflineClient.run_athena_query_pandas(queries_dict[client_name])
        except:
            configure_offline_feature_store(workgroup="development", catalog_name="production")
            initial_data = FeatureStoreOfflineClient.run_athena_query_pandas(queries_dict[client_name])
        
        # Below doesnt work because of work group conflicts so above is workaround:
        #acquire_initial_data = AcquireData(client=client_name, fname=f"{client_name}_data.p")
        #initial_data = acquire_initial_data.acquire_data()
    
    st.markdown("### Data Preview")
    st.dataframe(initial_data.head())

    st.markdown("### Select Columns for Analysis")
    with st.form(key="my_form"):
        ab = st.multiselect(
            "A/B Column",
            options=initial_data.columns,
            help="Select which column refers to A/B test labels.",
            default=ab_default,
        )
        result = st.multiselect(
            "Result Column",
            options=initial_data.columns,
            help="Select which column shows results of A/B test.",
            default=result_default,
        )

        # Change this, currently not used:
        #if ab:
        #    control = initial_data[ab[0]].unique()[0]
        #    treatment = initial_data[ab[0]].unique()[1]
        #    decide = st.radio(
        #        f"Is *{treatment}* Group B?",
        #        options=["Yes", "No"],
        #        help=f"Select yes if this is group B (or the treatment group) from your test.\nThus, {control} is Group A.",
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

    if not ab or not result:
        st.warning("Please select both an **A/B column** and a **Result column**.")
        st.stop()

    row0_space1, row0_col1, row0_space2, row0_col2, row0_space3 = st.columns(
        (0.05, 1, 0.05, 1, 0.05)
    )
    with row0_col1:
        st.write("")
        st.write("## AB Test Performance For:\n###", option)
        st.write("")
    with row0_col2:
        fit_dist = FitDistribution(fname=f"{client_name}_distribution_fit.p")
        best_distribution = fit_dist.fit(initial_data.loc[initial_data[result_default] > 0], result_default)
        st.write("")
        st.write("## Distribution Used (Best Fit):\n###", best_distribution)
        st.write("")
    
    # Create test results:
    result = ProducePredictions()
    results_conversion = result.produce_results_conversion(initial_data)
    results_revenue = result.produce_results_revenue(best_distribution, initial_data)
    results_posterior_sample = result._produce_results_lognorm_dist_carry_value(initial_data)
    
    # Set up metrics:
    post_sample_A      = results_posterior_sample[1]
    post_sample_B      = results_posterior_sample[0]
    post_sample_uplift = (post_sample_B - post_sample_A) / post_sample_A
    post_sample_diff   = post_sample_B - post_sample_A
    hdi_A              = az.hdi(post_sample_A, hdi_prob=st.session_state.hdi)
    hdi_B              = az.hdi(post_sample_B, hdi_prob=st.session_state.hdi)
    hdi_diff           = az.hdi(post_sample_uplift, hdi_prob=st.session_state.hdi)
    hdi_diff_ab        = az.hdi(post_sample_diff, hdi_prob=st.session_state.hdi)
    
    # Draw up tables:
    st.write("")
    row1_space1, row1_col1, row1_space2, row1_col2, row1_space3 = st.columns(
        (0.1, 1, 0.1, 1, 0.1)
    )
    with row1_col1:
        st.metric(
            "Delta ARPUs",
            value = "%.4f$" % (results_revenue[0]['avg_values'] - results_revenue[1]['avg_values']),
        )
    with row1_col2:
        st.metric(
            "Delta Conversion",
            value = "%.2f%%" % ((results_conversion[0]['positive_rate'] - results_conversion[1]['positive_rate']) * 100),
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
        fig_temp = sns.kdeplot(post_sample_A, color="blue", ax = ax)
        fig_temp = sns.kdeplot(post_sample_B, color="red", ax = ax)
        l1 = fig_temp.lines[0]
        l2 = fig_temp.lines[1]
        x1 = l1.get_xydata()[:,0]
        x2 = l2.get_xydata()[:,0]
        y1 = l1.get_xydata()[:,1]
        y2 = l2.get_xydata()[:,1]
        x1_new = x1[[all(tup) for tup in zip(list(x1 >= hdi_A[0]), list(x1 <= hdi_A[1]))]]
        x2_new = x2[[all(tup) for tup in zip(list(x2 >= hdi_B[0]), list(x2 <= hdi_B[1]))]]
        y1_new = y1[[all(tup) for tup in zip(list(x1 >= hdi_A[0]), list(x1 <= hdi_A[1]))]]
        y2_new = y2[[all(tup) for tup in zip(list(x2 >= hdi_B[0]), list(x2 <= hdi_B[1]))]]
        ax.fill_between(x1_new, y1_new, color="blue", alpha=0.3)
        ax.fill_between(x2_new, y2_new, color="red", alpha=0.3)
        ax.legend(labels=['Control','Personalised'])
        st.pyplot(fig)

    with row2_col2, _lock:
        st.subheader("Apporximate Distribution of Uplifts")
        fig2 = plt.figure.Figure()
        ax2 = fig2.add_subplot(111)
        fig_temp2 = sns.kdeplot(post_sample_uplift, color="purple", ax = ax2)
        l = fig_temp2.lines[0]
        x = l.get_xydata()[:,0]
        y = l.get_xydata()[:,1]
        x_new = x[[all(tup) for tup in zip(list(x >= hdi_diff[0]), list(x <= hdi_diff[1]))]]
        y_new = y[[all(tup) for tup in zip(list(x >= hdi_diff[0]), list(x <= hdi_diff[1]))]]
        ax2.xaxis.set_major_formatter(plt.ticker.PercentFormatter(xmax = 1, decimals = 2))
        ax2.fill_between(x_new, y_new, color="purple", alpha=0.3)
        st.pyplot(fig2)

    # Set up end tables:
    row3_space1, row3_col1, row3_space2, row3_col2, row3_space3 = st.columns(
        (0.05, 1, 0.05, 1, 0.05)
    )

    # Table1
    output_df = pd.DataFrame(columns=["Metric", "Conversion", "Revenue"])
    output_df["Metric"] = ["P( P > C)", "E( loss | P > C)", "E( loss | C > P)"]
    output_df["Conversion"] = [
        "%.2f%%" % (results_conversion[0]["prob_being_best"] * 100),
        "%.4f$" % (results_conversion[0]["expected_loss"]),
        "%.4f$" % (results_conversion[1]["expected_loss"]),
    ]
    output_df["Revenue"] = [
        "%.2f%%" % (results_revenue[0]["prob_being_best"] * 100),
        "%.4f$" % (results_revenue[0]["expected_loss"]),
        "%.4f$" % (results_revenue[1]["expected_loss"]),
    ]
    output_df = output_df.set_index('Metric')
    table1 = row3_col2.write(output_df)

    # Table2
    output_df2 = pd.DataFrame(columns=["Metric", "Control", "Personalised", "Personalised-Control"])
    output_df2["Metric"] = ["sample size", "conversion", "ARPU", "ARPPU", "95% HDI"]
    output_df2["Control"] = [
        "%d" % (results_revenue[1]['totals']),
        "%.2f%%" % (results_conversion[1]['positive_rate'] * 100),
        "%.4f$" % (results_revenue[1]['avg_values']),
        "%.4f$" % (results_revenue[1]['avg_positive_values']),
        "[%.4f$, %.4f$]" % (hdi_A[0], hdi_A[1]),
    ]
    output_df2["Personalised"] = [
        "%d" % (results_revenue[0]['totals']),
        "%.2f%%" % (results_conversion[0]['positive_rate'] * 100),
        "%.4f$" % (results_revenue[0]['avg_values']),
        "%.4f$" % (results_revenue[0]['avg_positive_values']),
        "[%.4f$, %.4f$]" % (hdi_B[0], hdi_B[1]),
    ]
    output_df2["Personalised-Control"] = [
        np.NAN,
        "%.2f%%" % ((results_conversion[0]['positive_rate'] - results_conversion[1]['positive_rate']) * 100),
        "%.4f$" % (results_revenue[0]['avg_values'] - results_revenue[1]['avg_values']),
        "%.4f$" % (results_revenue[0]['avg_positive_values'] - results_revenue[1]['avg_positive_values']),
        "[%.4f$, %.4f$]" % (hdi_diff_ab[0], hdi_diff_ab[1]),
    ]
    output_df2 = output_df2.set_index('Metric')
    table2 = row3_col1.write(output_df2)
