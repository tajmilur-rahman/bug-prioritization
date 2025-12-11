import numpy as np
import plotly.graph_objects as go
import plotly.io as pio


def make_ig_bar_chart(feature_names, ig_pred, ig_true, pred_class, true_class, out_html):
    """
    Creates interactive IG comparison bar chart and saves HTML.
    """
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=feature_names,
        y=ig_pred,
        name=f"Predicted class {pred_class}",
        marker_color="blue",
        opacity=0.7
    ))

    fig.add_trace(go.Bar(
        x=feature_names,
        y=ig_true,
        name=f"True class {true_class}",
        marker_color="red",
        opacity=0.7
    ))

    fig.update_layout(
        title=f"IG Comparison: Pred={pred_class} vs True={true_class}",
        xaxis_tickangle=90,
        template="plotly_white",
        width=1400,
        height=500
    )

    pio.write_html(fig, file=out_html, auto_open=False)
    return out_html


def make_block_level_chart(block_schema, ig_pred, ig_true, out_html):
    block_names = [b["name"] for b in block_schema]
    pred_vals = []
    true_vals = []

    for b in block_schema:
        s, e = b["start"], b["end"]
        pred_vals.append(float(np.sum(np.abs(ig_pred[s:e]))))
        true_vals.append(float(np.sum(np.abs(ig_true[s:e]))))

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=block_names,
        y=pred_vals,
        name="Predicted class IG"
    ))

    fig.add_trace(go.Bar(
        x=block_names,
        y=true_vals,
        name="True class IG"
    ))

    fig.update_layout(
        title="Block-Level IG Attribution",
        template="plotly_white",
        width=900,
        height=500
    )

    pio.write_html(fig, file=out_html, auto_open=False)
    return out_html
