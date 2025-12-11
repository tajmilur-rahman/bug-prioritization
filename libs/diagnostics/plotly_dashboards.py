import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import pandas as pd
import json

def plotly_feature_importance(ig_mean, feature_names, save_path):
    k = len(ig_mean)

    fig = go.Figure(
        data=[
            go.Bar(
                x=feature_names,
                y=ig_mean,
                marker=dict(color=ig_mean, colorscale="Viridis"),
            )
        ]
    )
    fig.update_layout(
        title="Global IG Feature Importance",
        xaxis_title="Features",
        yaxis_title="Integrated Gradient (mean |IG|)"
    )
    fig.write_html(save_path)


def plotly_block_importance(block_importance, save_path):
    blk = list(block_importance.keys())
    vals = list(block_importance.values())

    fig = go.Figure(
        data=[go.Bar(x=blk, y=vals)]
    )
    fig.update_layout(
        title="Block-Level Importance",
        xaxis_title="Block",
        yaxis_title="Importance"
    )
    fig.write_html(save_path)


def plotly_topic_purity(purity_dict, save_path):
    topics = list(purity_dict.keys())
    purity = list(purity_dict.values())
    fig = px.bar(x=topics, y=purity)
    fig.update_layout(
        title="Topic Purity",
        xaxis_title="Topic",
        yaxis_title="Purity"
    )
    fig.write_html(save_path)

