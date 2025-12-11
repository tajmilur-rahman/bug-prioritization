from datetime import datetime
import json

def write_diagnostic_report(
    out_path,
    pred_true_stats,
    block_summary,
    clusters,
    global_stats
):
    """
    Writes an HTML diagnostic report summarizing all IG insights.
    """
    tstamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""
    <html>
    <head>
        <title>Model Diagnostic Report</title>
        <style>
        body {{ font-family: Arial; margin: 30px; }}
        h1 {{ color: #333; }}
        pre {{ background: #f0f0f0; padding: 10px; }}
        </style>
    </head>
    <body>
        <h1>Model Diagnostic Report</h1>
        <p>Generated at: {tstamp}</p>

        <h2>Prediction Summary</h2>
        <pre>{json.dumps(pred_true_stats, indent=2)}</pre>

        <h2>Block-Level IG Summary</h2>
        <pre>{json.dumps(block_summary, indent=2)}</pre>

        <h2>Error Clusters</h2>
        <pre>{json.dumps(clusters, indent=2)}</pre>

        <h2>Global IG Statistics</h2>
        <pre>{json.dumps(global_stats, indent=2)}</pre>

        <p>End of Report</p>
    </body>
    </html>
    """

    with open(out_path, "w") as f:
        f.write(html)

    return out_path
