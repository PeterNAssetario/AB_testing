import time
from io import BytesIO
from datetime import datetime, timedelta

import numpy as np
import scipy
import altair as alt
import pandas as pd
import seaborn as sns
import streamlit as st
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from numpy.random import default_rng


def unpack_columns_from_dict(df, dict_column_name, new_column_names):

    for new_column_name in new_column_names:
        df[new_column_name] = 0
        for i in range(df.shape[0]):
            df.loc[i, new_column_name] = df.loc[i, dict_column_name][new_column_name]

    return df


st.set_page_config(
    page_title="AB testing results",
    page_icon="📊",
    initial_sidebar_state="expanded",
    layout="wide",
)

st.write(
    """
# 📊 Bayesian A/B Testing Results
"""
)


all_results_df = pd.read_parquet("fake_ab_testing_results/all_results_df")
st.dataframe(all_results_df)

variant_name_1 = "P"
variant_name_2 = "C"


for i in range(all_results_df.shape[0]):
    st.header(all_results_df.iloc[i].test_name)
    columns = st.columns([1, 1, 1, 2])
    columns[0].metric(
        f"Expected Loss for variant {variant_name_1}",
        np.round(all_results_df.iloc[i].expected_loss_version_1, 5),
        delta=np.round(all_results_df.iloc[i].expected_loss_delta_version_1, 5),
        delta_color="inverse",
    )
    columns[1].metric(
        f"Probability of being better for variant {variant_name_1}",
        np.round(all_results_df.iloc[i].prob_being_best_version_1, 5),
        delta=np.round(all_results_df.iloc[i].prob_being_best_delta_version_1, 5),
        delta_color="normal",
    )
    columns[2].metric(
        f"Expected Total Gain for variant {variant_name_1}",
        np.round(all_results_df.iloc[i].expected_total_gain_version_1, 5),
        delta=np.round(all_results_df.iloc[i].expected_total_gain_delta_version_1, 5),
        delta_color="normal",
    )

    with st.expander(f"Results for variant {variant_name_2}"):
        columns = st.columns([1, 1, 1, 2])
        columns[0].metric(
            f"Expected Loss for variant {variant_name_2}",
            np.round(all_results_df.iloc[i].expected_loss_version_2, 5),
            delta=np.round(all_results_df.iloc[i].expected_loss_delta_version_2, 5),
            delta_color="inverse",
        )
        columns[1].metric(
            f"Probability of being better for variant {variant_name_2}",
            np.round(all_results_df.iloc[i].prob_being_best_version_2, 5),
            delta=np.round(all_results_df.iloc[i].prob_being_best_delta_version_2, 5),
            delta_color="normal",
        )
        columns[2].metric(
            f"Expected Total Gain for variant {variant_name_2}",
            np.round(all_results_df.iloc[i].expected_total_gain_version_2, 5),
            delta=np.round(
                all_results_df.iloc[i].expected_total_gain_delta_version_2, 5
            ),
            delta_color="normal",
        )


option = st.selectbox(
    "",
    (all_results_df.test_name.values),
    help="Results of which AB test would you like to view?",
    index=0,
)


client_map = {
    "test_1": "test_1_results",
    "test_2": "test_2_results",
    "test_3": "test_3_results",
    "test_4": "test_4_results",
    "test_5": "test_5_results",
    "test_6": "test_6_results",
    "test_7": "test_7_results",
}

results_df = pd.read_parquet("fake_ab_testing_results/" + client_map[option])

results_df = unpack_columns_from_dict(
    results_df,
    "inputs",
    [
        "totals_variant_1",
        "totals_variant_2",
        "positives_variant_1",
        "positives_variant_2",
        "sum_values_variant_1",
        "sum_values_variant_2",
        "sum_logs_variant_1",
        "sum_logs_variant_2",
        "sum_logs_squared_variant_1",
        "sum_logs_squared_variant_2",
        "a_prior_beta_variant_1",
        "a_prior_beta_variant_2",
        "b_prior_beta_variant_1",
        "b_prior_beta_variant_2",
        "m_prior_variant_1",
        "m_prior_variant_2",
        "a_prior_ig_variant_1",
        "a_prior_ig_variant_2",
        "b_prior_ig_variant_1",
        "b_prior_ig_variant_2",
        "w_prior_variant_1",
        "w_prior_variant_2",
    ],
)

results_df = unpack_columns_from_dict(
    results_df,
    "results",
    [
        "prob_being_best_version_1",
        "prob_being_best_version_2",
        "expected_loss_version_1",
        "expected_loss_version_2",
        "expected_total_gain_version_1",
        "expected_total_gain_version_2",
    ],
)

min_date = results_df.meta_date.min().to_pydatetime()
max_date = results_df.meta_date.max().to_pydatetime()


st.dataframe(results_df.head())

### CURRENT DATE
# current_date = st.sidebar.slider(
#     "When do you start?",
#     value=max_date,
#     format="MM/DD/YY",
#     min_value=min_date,
#     max_value=max_date,
# )

tab1, tab2, tab3 = st.tabs(["Test results", "Posteriors", "Data"])

with tab1:
    tab1.header("Test results")

    with st.container():
        st.subheader("Probability of being the best")
        source = results_df[
            ["meta_date", "prob_being_best_version_1", "prob_being_best_version_2"]
        ].melt("meta_date", var_name="variant", value_name="probab of being best")

        source["variant"] = source["variant"].apply(
            lambda x: variant_name_1
            if x == "prob_being_best_version_1"
            else variant_name_2
        )
        # source["x"] = source["x"].dt.day

        selection = alt.selection_multi(fields=["variant"])
        nearest = alt.selection(
            type="single",
            nearest=True,
            on="mouseover",
            fields=["meta_date"],
            empty="none",
        )

        # The basic line
        line = (
            alt.Chart(source)
            .mark_line()
            .encode(x="meta_date:T", y="probab of being best:Q", color="variant:N")
        )

        # Transparent selectors across the chart. This is what tells us
        # the x-value of the cursor
        selectors = (
            alt.Chart(source)
            .mark_point()
            .encode(
                x="meta_date:T",
                opacity=alt.value(0),
            )
            .add_selection(nearest)
        )

        # Draw points on the line, and highlight based on selection
        points = line.mark_point().encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0))
        )

        # Draw text labels near the points, and highlight based on selection
        text = line.mark_text(align="left", dx=5, dy=-5).encode(
            text=alt.condition(nearest, "probab of being best:Q", alt.value(" "))
        )

        # Draw a rule at the location of the selection
        rules = (
            alt.Chart(source)
            .mark_rule(color="gray")
            .encode(
                x="meta_date:T",
            )
            .transform_filter(nearest)
        )

        # Put the five layers into a chart and bind the data
        fig = alt.layer(line, selectors, points, rules, text).properties(
            width=900, height=300
        )

        st.altair_chart(fig)

    with st.container():
        st.subheader("Expected loss")
        source = results_df[
            ["meta_date", "expected_loss_version_1", "expected_loss_version_2"]
        ].melt("meta_date", var_name="variant", value_name="Expected loss")

        source["variant"] = source["variant"].apply(
            lambda x: variant_name_1
            if x == "expected_loss_version_1"
            else variant_name_2
        )

        selection = alt.selection_multi(fields=["variant"])
        nearest = alt.selection(
            type="single",
            nearest=True,
            on="mouseover",
            fields=["meta_date"],
            empty="none",
        )

        # The basic line
        line = (
            alt.Chart(source)
            .mark_line()
            .encode(x="meta_date:T", y="Expected loss:Q", color="variant:N")
        )

        # Transparent selectors across the chart. This is what tells us
        # the x-value of the cursor
        selectors = (
            alt.Chart(source)
            .mark_point()
            .encode(
                x="meta_date:T",
                opacity=alt.value(0),
            )
            .add_selection(nearest)
        )

        # Draw points on the line, and highlight based on selection
        points = line.mark_point().encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0))
        )

        # Draw text labels near the points, and highlight based on selection
        text = line.mark_text(align="left", dx=5, dy=-5).encode(
            text=alt.condition(nearest, "Expected loss:Q", alt.value(" "))
        )

        # Draw a rule at the location of the selection
        rules = (
            alt.Chart(source)
            .mark_rule(color="gray")
            .encode(
                x="meta_date:T",
            )
            .transform_filter(nearest)
        )

        # Put the five layers into a chart and bind the data
        fig = alt.layer(line, selectors, points, rules, text).properties(
            width=900, height=300
        )

        st.altair_chart(fig)

    with st.container():
        st.subheader("Expected total gain")
        source = results_df[
            [
                "meta_date",
                "expected_total_gain_version_1",
                "expected_total_gain_version_2",
            ]
        ].melt("meta_date", var_name="variant", value_name="Expected total gain")

        source["variant"] = source["variant"].apply(
            lambda x: variant_name_1
            if x == "expected_total_gain_version_1"
            else variant_name_2
        )

        selection = alt.selection_multi(fields=["variant"])
        nearest = alt.selection(
            type="single",
            nearest=True,
            on="mouseover",
            fields=["meta_date"],
            empty="none",
        )

        # The basic line
        line = (
            alt.Chart(source)
            .mark_line()
            .encode(x="meta_date:T", y="Expected total gain:Q", color="variant:N")
        )

        # Transparent selectors across the chart. This is what tells us
        # the x-value of the cursor
        selectors = (
            alt.Chart(source)
            .mark_point()
            .encode(
                x="meta_date:T",
                opacity=alt.value(0),
            )
            .add_selection(nearest)
        )

        # Draw points on the line, and highlight based on selection
        points = line.mark_point().encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0))
        )

        # Draw text labels near the points, and highlight based on selection
        text = line.mark_text(align="left", dx=5, dy=-5).encode(
            text=alt.condition(nearest, "Expected total gain:Q", alt.value(" "))
        )

        # Draw a rule at the location of the selection
        rules = (
            alt.Chart(source)
            .mark_rule(color="gray")
            .encode(
                x="meta_date:T",
            )
            .transform_filter(nearest)
        )

        # Put the five layers into a chart and bind the data
        fig = alt.layer(line, selectors, points, rules, text).properties(
            width=900, height=300
        )

        st.altair_chart(fig)


with tab2:
    tab2.header("Posteriors")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Posteriors for the conversion rate")
        full_df = pd.DataFrame(columns=["x", "y", "meta_date", "variant"])
        min_list = []
        max_list = []

        for current_date in results_df.meta_date.unique():
            print(current_date)
            a_param_beta_1 = results_df[
                results_df.meta_date == current_date
            ].a_post_beta_variant_1.values[0]
            a_param_beta_2 = results_df[
                results_df.meta_date == current_date
            ].a_post_beta_variant_2.values[0]

            b_param_beta_1 = results_df[
                results_df.meta_date == current_date
            ].b_post_beta_variant_1.values[0]
            b_param_beta_2 = results_df[
                results_df.meta_date == current_date
            ].b_post_beta_variant_2.values[0]

            min_boundary_1 = scipy.stats.beta.ppf(
                0.01,
                a_param_beta_1,
                b_param_beta_1,
            )
            min_list.append(min_boundary_1)
            min_boundary_2 = scipy.stats.beta.ppf(
                0.01,
                a_param_beta_2,
                b_param_beta_2,
            )
            min_list.append(min_boundary_2)

            max_boundary_1 = scipy.stats.beta.ppf(
                0.99,
                a_param_beta_1,
                b_param_beta_1,
            )
            max_list.append(max_boundary_1)
            max_boundary_2 = scipy.stats.beta.ppf(
                0.99,
                a_param_beta_2,
                b_param_beta_2,
            )
            max_list.append(max_boundary_2)

        for current_date in results_df.meta_date.unique():
            print(current_date)
            a_param_beta = results_df[
                results_df.meta_date == current_date
            ].a_post_beta_variant_1.values[0]
            b_param_beta = results_df[
                results_df.meta_date == current_date
            ].b_post_beta_variant_1.values[0]

            min_boundary = scipy.stats.beta.ppf(
                0.01,
                a_param_beta,
                b_param_beta,
            )

            max_boundary = scipy.stats.beta.ppf(
                0.99,
                a_param_beta,
                b_param_beta,
            )

            diff = max(max_list) - min(min_list)

            x_middle = np.linspace(min_boundary, max_boundary, 50)
            x_low = np.linspace(min(min_list) - diff / 10, min_boundary, 50)
            x_high = np.linspace(max_boundary, max(max_list) + diff / 10, 50)
            x = np.concatenate((x_low, x_middle, x_high))

            y = scipy.stats.beta.pdf(x, a_param_beta, b_param_beta)
            temp_df = pd.DataFrame({"x": x, "y": y})
            temp_df["meta_date"] = current_date
            temp_df["variant"] = "P"

            full_df = pd.concat([full_df, temp_df])

            a_param_beta = results_df[
                results_df.meta_date == current_date
            ].a_post_beta_variant_2.values[0]
            b_param_beta = results_df[
                results_df.meta_date == current_date
            ].b_post_beta_variant_2.values[0]

            min_boundary = scipy.stats.beta.ppf(
                0.01,
                a_param_beta,
                b_param_beta,
            )

            max_boundary = scipy.stats.beta.ppf(
                0.99,
                a_param_beta,
                b_param_beta,
            )

            diff = max(max_list) - min(min_list)

            x_middle = np.linspace(min_boundary, max_boundary, 50)
            x_low = np.linspace(min(min_list) - diff / 10, min_boundary, 50)
            x_high = np.linspace(max_boundary, max(max_list) + diff / 10, 50)
            x = np.concatenate((x_low, x_middle, x_high))

            y = scipy.stats.beta.pdf(x, a_param_beta, b_param_beta)
            temp_df = pd.DataFrame({"x": x, "y": y})
            temp_df["meta_date"] = current_date
            temp_df["variant"] = "C"

            full_df = pd.concat([full_df, temp_df])

        movies = full_df.copy()
        movies = movies[
            (movies.x >= min(min_list) - diff / 10)
            & (movies.x <= max(max_list) + diff / 10)
        ]

        movies = movies.rename(columns={"x": "xko", "y": "yko"})

        x_init = (
            pd.to_datetime(
                [
                    max(results_df.meta_date) - timedelta(hours=23),
                    max(results_df.meta_date),
                ]
            ).astype(int)
            / 1e6
        )
        select_year = alt.selection_interval(encodings=["x"], init={"x": list(x_init)})

        bar_slider = (
            alt.Chart(movies)
            .mark_bar()
            .encode(x="meta_date", y="count()")
            .properties(height=50)
            .add_selection(select_year)
        )

        scatter_plot = (
            alt.Chart(movies)
            .mark_line()
            .encode(
                x="xko:Q",
                y="yko:Q",
                color="meta_date",
                strokeDash="variant",
                opacity=alt.condition(select_year, alt.value(0.7), alt.value(0.001)),
            )
        )

        scatter_plot & bar_slider

    with col2:
        a_beta_variant_1 = results_df[
            results_df.meta_date == max(results_df.meta_date)
        ].a_post_beta_variant_1.values[0]
        b_beta_variant_1 = results_df[
            results_df.meta_date == max(results_df.meta_date)
        ].b_post_beta_variant_1.values[0]

        a_beta_variant_2 = results_df[
            results_df.meta_date == max(results_df.meta_date)
        ].a_post_beta_variant_2.values[0]
        b_beta_variant_2 = results_df[
            results_df.meta_date == max(results_df.meta_date)
        ].b_post_beta_variant_2.values[0]

        min_boundary_1 = scipy.stats.beta.ppf(
            0.01,
            a_beta_variant_1,
            b_beta_variant_1,
        )
        max_boundary_1 = scipy.stats.beta.ppf(
            0.99,
            a_beta_variant_1,
            b_beta_variant_1,
        )

        min_boundary_2 = scipy.stats.beta.ppf(
            0.01,
            a_beta_variant_2,
            b_beta_variant_2,
        )
        max_boundary_2 = scipy.stats.beta.ppf(
            0.99,
            a_beta_variant_2,
            b_beta_variant_2,
        )

        min_boundary = min(min_boundary_1, min_boundary_2)
        max_boundary = max(max_boundary_1, max_boundary_2)
        diff = max_boundary - min_boundary

        x_middle = np.linspace(min_boundary_1, max_boundary_1, 50)
        x_low = np.linspace(min_boundary - diff / 3, min_boundary_1, 50)
        x_high = np.linspace(max_boundary_1, max_boundary + diff / 3, 50)
        x_vector = np.concatenate((x_low, x_middle, x_high))

        y_middle = np.linspace(min_boundary_2, max_boundary_2, 50)
        y_low = np.linspace(min_boundary - diff / 3, min_boundary_2, 50)
        y_high = np.linspace(max_boundary_2, max_boundary + diff / 3, 50)
        y_vector = np.concatenate((y_low, y_middle, y_high))

        z_pdf = np.zeros((len(x_vector), len(y_vector)))
        for i, x in enumerate(x_vector):
            for j, y in enumerate(y_vector):
                z_pdf[i, j] = scipy.stats.beta.pdf(
                    x, a_beta_variant_1, b_beta_variant_1
                ) * scipy.stats.beta.pdf(y, a_beta_variant_2, b_beta_variant_2)

        # fig, ax = plt.subplots(figsize=(5, 5))
        # X, Y = np.meshgrid(x_vector, x_vector)
        # ax.contourf(X, Y, np.transpose(z_pdf))
        # ax.set_xlabel("theta_C")
        # ax.set_ylabel("theta_P")
        # st.pyplot(fig)

        import plotly.graph_objects as go

        fig = go.Figure(
            data=go.Heatmap(
                z=z_pdf,
                x=x_vector,
                y=y_vector,
                zsmooth="best",  # horizontal axis  # vertical axis
            )
        )

        fig.add_trace(
            go.Scatter(
                x=[min_boundary - diff / 3, max_boundary + diff / 3],
                y=[min_boundary - diff / 3, max_boundary + diff / 3],
                mode="lines",
                name="Female",
            )
        )

        fig.update_layout(
            autosize=True,
            width=500,
            height=500,
        )
        st.plotly_chart(fig, use_container_width=False)

    with col1:
        st.subheader("Posteriors for the means of revenue")

        from numpy.random import default_rng

        full_df = pd.DataFrame(columns=["x", "y", "meta_date", "variant"])
        variant_names = ["P", "C"]

        list_min_values = []
        list_max_values = []
        fitted_kdes = []
        for current_date in results_df.meta_date.unique():
            rng = default_rng(seed=293809384)

            a_params_ig = (
                results_df[
                    results_df.meta_date == current_date
                ].a_post_ig_variant_1.values[0],
                results_df[
                    results_df.meta_date == current_date
                ].a_post_ig_variant_2.values[0],
            )
            b_params_ig = (
                results_df[
                    results_df.meta_date == current_date
                ].b_post_ig_variant_1.values[0],
                results_df[
                    results_df.meta_date == current_date
                ].b_post_ig_variant_2.values[0],
            )

            m_params = (
                results_df[
                    results_df.meta_date == current_date
                ].m_post_variant_1.values[0],
                results_df[
                    results_df.meta_date == current_date
                ].m_post_variant_2.values[0],
            )
            w_params = (
                results_df[
                    results_df.meta_date == current_date
                ].w_post_variant_1.values[0],
                results_df[
                    results_df.meta_date == current_date
                ].w_post_variant_2.values[0],
            )

            fitted_kdes_small = []
            for i in range(2):
                sampled_variances = 1 / rng.gamma(
                    a_params_ig[i], 1 / b_params_ig[i], 10000
                )
                sampled_means = []
                # for each variance generate a mean
                for sampled_variance in sampled_variances:
                    sampled_means.append(
                        rng.normal(m_params[i], np.sqrt(sampled_variance / w_params[i]))
                    )

                # compute the means of the lognormal distribution
                sampled_means_of_lognormal = np.exp(
                    sampled_means + sampled_variances / 2
                )

                # plot
                min_value = np.quantile(sampled_means_of_lognormal, 0.01)
                max_value = np.quantile(sampled_means_of_lognormal, 0.99)

                list_min_values.append(min_value)
                list_max_values.append(max_value)

                fitted_kde = scipy.stats.gaussian_kde(
                    sampled_means_of_lognormal, bw_method=None, weights=None
                )
                fitted_kdes_small.append(fitted_kde)

            fitted_kdes.append(fitted_kdes_small)

        diff = max(list_max_values) - min(list_min_values)

        for j, current_date in enumerate(results_df.meta_date.unique()):
            for i in range(2):
                x = np.linspace(
                    min(list_min_values) - diff / 10,
                    max(list_max_values) + diff / 10,
                    100,
                )

                fitted_kde = fitted_kdes[j][i]
                y = fitted_kde.pdf(x)

                temp_df = pd.DataFrame({"x": x, "y": y})
                temp_df["meta_date"] = current_date
                temp_df["variant"] = variant_names[i]

                full_df = pd.concat([full_df, temp_df])

        movies = full_df.copy()
        # movies = movies[
        #             (movies.x >= min(min_list) - diff / 10)
        #             & (movies.x <= max(max_list) + diff / 10)
        #         ]

        movies = movies.rename(columns={"x": "xko", "y": "yko"})

        x_init = (
            pd.to_datetime(
                [
                    max(results_df.meta_date) - timedelta(hours=23),
                    max(results_df.meta_date),
                ]
            ).astype(int)
            / 1e6
        )
        select_year = alt.selection_interval(encodings=["x"], init={"x": list(x_init)})

        bar_slider = (
            alt.Chart(movies)
            .mark_bar()
            .encode(x="meta_date", y="count()")
            .properties(height=50)
            .add_selection(select_year)
        )

        scatter_plot = (
            alt.Chart(movies)
            .mark_line()
            .encode(
                x="xko:Q",
                y="yko:Q",
                color="meta_date",
                strokeDash="variant",
                opacity=alt.condition(select_year, alt.value(0.7), alt.value(0.001)),
            )
        )

        scatter_plot & bar_slider


with tab3:
    tab3.header("Info about data")
    ### NUMBER OF DATAPOINTS PER DAY ######################################
    with st.container():
        st.write("#### Number of datapoints per day")
        fig, ax = plt.subplots(figsize=(20, 3))
        x = np.linspace(0, 1, 100)
        sns.lineplot(
            x=results_df.meta_date.dt.date,
            y=results_df.totals_variant_1,
            color="blue",
        )
        sns.lineplot(
            x=results_df.meta_date.dt.date,
            y=results_df.totals_variant_2,
            color="red",
        )
        st.pyplot(fig)
    #########################################################################

    ### TOTAL NUMBER OF DATAPOINTS PER DAY ######################################
    a = [sum(results_df.totals_variant_1[0:i]) for i in range(results_df.shape[0])]
    with st.container():
        st.write("#### Total number of datapoints")
        fig, ax = plt.subplots(figsize=(20, 3))
        x = np.linspace(0, 1, 100)
        sns.lineplot(
            x=results_df.meta_date.dt.date,
            y=a,
            color="blue",
        )
        st.pyplot(fig)
    #########################################################################


# ### trying interactive plotting
# with st.container():
#     full_df = pd.DataFrame(columns=["x", "y", "meta_date"])

#     for curr_date in results_df.meta_date.unique():
#         print(curr_date)
#         a_param_beta = results_df[
#             results_df.meta_date == curr_date
#         ].a_post_beta_variant_1.values[0]
#         b_param_beta = results_df[
#             results_df.meta_date == curr_date
#         ].b_post_beta_variant_1.values[0]

#         x = np.linspace(
#             scipy.stats.beta.ppf(
#                 0.01,
#                 a_param_beta,
#                 b_param_beta,
#             ),
#             scipy.stats.beta.ppf(
#                 0.99,
#                 a_param_beta,
#                 b_param_beta,
#             ),
#             100,
#         )

#         y = scipy.stats.beta.pdf(x, a_param_beta, b_param_beta)
#         temp_df = pd.DataFrame({"x": x, "y": y})
#         temp_df["meta_date"] = curr_date

#         full_df = pd.concat([full_df, temp_df])

#     fig = (
#         alt.Chart(full_df[full_df.meta_date == current_date])
#         .mark_line()
#         .encode(x="x", y="y", color="meta_date")
#         .interactive()
#     )
#     st.altair_chart(fig)


# # import plotly.express as px

# # df = px.data.gapminder()
# # fig = px.scatter(
# #     df,
# #     x="gdpPercap",
# #     y="lifeExp",
# #     animation_frame="year",
# #     animation_group="country",
# #     size="pop",
# #     color="continent",
# #     hover_name="country",
# #     log_x=True,
# #     size_max=55,
# #     range_x=[100, 100000],
# #     range_y=[25, 90],
# # )

# # st.plotly_chart(fig)


# st.write("# Posteriors")


# progress_bar = st.progress(0)
# status_text = st.empty()
# chart = st.line_chart(np.random.randn(10, 2))

# for i in range(100):
#     # Update progress bar.
#     progress_bar.progress(i + 1)

#     new_rows = np.random.randn(10, 2)

#     # Update status text.
#     status_text.text("The latest random number is: %s" % new_rows[-1, 1])

#     # Append data to the chart.
#     chart.add_rows(new_rows)

#     # Pretend we're doing some computation that takes time.
#     time.sleep(0.001)

# status_text.text("Done!")
# # st.balloons()
