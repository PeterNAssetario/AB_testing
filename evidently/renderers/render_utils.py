from typing import Optional

import plotly.graph_objs as go

from evidently.options import ColorOptions
from evidently.utils.visualizations import Distribution

import pandas as pd


def plot_distr(*, hist_curr, hist_ref=None, orientation="v", color_options: ColorOptions):
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name="current",
            x=hist_curr["x"],
            y=hist_curr["count"],
            marker_color=color_options.get_current_data_color(),
            orientation=orientation,
        )
    )
    if hist_ref is not None:
        fig.add_trace(
            go.Bar(
                name="reference",
                x=hist_ref["x"],
                y=hist_ref["count"],
                marker_color=color_options.get_reference_data_color(),
                orientation=orientation,
            )
        )

    return fig


def get_distribution_plot_figure(
    *,
    current_distribution: Distribution,
    reference_distribution: Optional[Distribution],
    color_options: ColorOptions,
    orientation: str = "v",
) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="current",
            x=current_distribution.x,
            y=current_distribution.y,
            marker_color=color_options.get_current_data_color(),
            orientation=orientation,
        )
    )
    if reference_distribution is not None:
        fig.add_trace(
            go.Bar(
                name="reference",
                x=reference_distribution.x,
                y=reference_distribution.y,
                marker_color=color_options.get_reference_data_color(),
                orientation=orientation,
            )
        )

    return fig


def get_distribution_with_time_plot_figure(
    *,
    res,
    # current_distribution: Distribution,
    # reference_distribution: Optional[Distribution],
    # color_options: ColorOptions,
) -> go.Figure:
    df = pd.read_csv("/Users/PeterNovak/Desktop/ab-testing/hour.csv")
    fig = go.Figure()
    for hr in range(0,24):
        zone_a = df[(df['hr']==hr)&(df['season']==1)][res.feature_name]
        zone_b = df[(df['hr']==hr)&(df['season']==2)][res.feature_name]
        fig.add_trace(go.Violin(y=zone_a, line_color='blue', name=f'Hour {hr}', showlegend=False, legendgroup="Spring"))
        fig.add_trace(go.Violin(y=zone_b, line_color='orange', name=f'Hour {hr}', showlegend=False, legendgroup="Summer"))

    fig.update_traces(orientation='v', side='positive', width=2, points=False)
    fig.update_layout(xaxis_showgrid=False, xaxis_zeroline=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")

    fig.add_trace(go.Bar(x=[np.nan], y=[np.nan], legendgroup="Spring", marker_color='blue', name="Spring"))
    fig.add_trace(go.Bar(x=[np.nan], y=[np.nan], legendgroup="Summer", marker_color='orange',  name="Summer"))
    
    return fig