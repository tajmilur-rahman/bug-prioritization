# reporting.py
import os, json, time
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.metrics import (
    f1_score, classification_report, confusion_matrix,
    precision_recall_curve, average_precision_score
)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def now_ts():
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())

def make_run_dir(model_tag: str, artifacts_root: str = "artifacts"):
    run_dir = Path(artifacts_root) / f"clf_{model_tag}_{now_ts()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "figs").mkdir(parents=True, exist_ok=True)
    return run_dir

def save_label_map(run_dir: Path, labels, label_names):
    lm = {int(l): str(n) for l, n in zip(np.asarray(labels).tolist(), list(label_names))}
    (run_dir / "label_map.json").write_text(json.dumps(lm, indent=2), encoding="utf-8")
    return lm

def save_input_data_information(run_dir: Path, ytr: np.ndarray, yva: np.ndarray, prep_id: str):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,6))
    grouped_ytr = pd.Series(ytr).value_counts()
    ax1.pie(grouped_ytr.values, labels=grouped_ytr.index, 
            autopct=lambda p: f'{p:.1f}%\n({(p/100)*sum(grouped_ytr.values):.0f})')
    ax1.set_title("Train Labels Distribution")
    grouped_yva = pd.Series(yva).value_counts()
    ax2.pie(grouped_yva.values, labels=grouped_yva.index, 
            autopct=lambda p: f'{p:.1f}%\n\n({(p/100)*sum(grouped_yva.values):.0f})')
    ax2.set_title("Evaluation Labels Distribution")

    # save input dataset info
    input_json = {
        "train_labels_distribution": {k: int(c) for k, c in zip(grouped_ytr.index, grouped_ytr.values)},
        "val_labels_distribution": {k: int(c) for k, c in zip(grouped_yva.index, grouped_yva.values)},
        "prep_id": prep_id
    }
    (run_dir / "input_info.json").write_text(json.dumps(input_json, indent=2), encoding="utf-8")

    # save figure to file
    fig.tight_layout()
    fig_path = run_dir / "figs" / "Train - Val Labels Distribution.png"
    fig.savefig(fig_path, dpi=140)
    plt.close(fig)

def _safe_list(obj):
    return obj.tolist() if hasattr(obj, "tolist") else list(obj)

def _compute_ece(probs: np.ndarray, y_true: np.ndarray, n_bins: int = 15) -> float:
    confidences = probs.max(axis=1)
    preds = probs.argmax(axis=1)
    correct = (preds == y_true).astype(float)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bins[i], bins[i+1]
        mask = (confidences > lo) & (confidences <= hi) if i>0 else (confidences >= lo) & (confidences <= hi)
        if not np.any(mask): 
            continue
        acc = correct[mask].mean()
        conf = confidences[mask].mean()
        ece += (mask.mean()) * abs(acc - conf)
    return float(ece)

def _brier_multiclass(probs: np.ndarray, y_true: np.ndarray) -> float:
    n, k = probs.shape
    Y = np.zeros_like(probs)
    Y[np.arange(n), y_true] = 1.0
    return float(np.mean(np.sum((probs - Y) ** 2, axis=1)))

def _save_confusion(run_dir: Path, y_true, y_pred, labels, label_names):
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    cm_json = {
        "labels": _safe_list(labels),
        "label_names": list(label_names),
        "matrix": cm.tolist()
    }
    (run_dir / "confusion_matrix.json").write_text(json.dumps(cm_json, indent=2), encoding="utf-8")

    fig = plt.figure(figsize=(6,5))
    ax = fig.add_subplot(111)
    ax.imshow(cm, interpolation="nearest")
    ax.set_xticks(range(len(labels))); ax.set_yticks(range(len(labels)))
    ax.set_xticklabels(label_names, rotation=45, ha="right")
    ax.set_yticklabels(label_names)
    ax.set_xlabel("Predicted"); ax.set_ylabel("True"); ax.set_title("Confusion Matrix")
    for (i,j), val in np.ndenumerate(cm):
        ax.text(j, i, int(val), ha="center", va="center")
    fig.tight_layout()
    fig_path = run_dir / "figs" / "confusion_matrix.png"
    fig.savefig(fig_path, dpi=140)
    plt.close(fig)
    return cm_json, str(fig_path)

def _save_pr_curves(run_dir: Path, y_true, probs, labels, label_names):
    curves = {}
    for idx, lbl in enumerate(labels):
        y_bin = (y_true == lbl).astype(int)
        precision, recall, _ = precision_recall_curve(y_bin, probs[:, idx])
        ap = float(average_precision_score(y_bin, probs[:, idx]))
        curves[str(lbl)] = {
            "average_precision": ap,
            "precision": precision.tolist(),
            "recall": recall.tolist()
        }
        fig = plt.figure(figsize=(5,4))
        ax = fig.add_subplot(111)
        ax.plot(recall, precision)
        ax.set_xlabel("Recall"); ax.set_ylabel("Precision")
        ax.set_title(f"PR Curve: {label_names[idx]} (AP={ap:.3f})")
        fig.tight_layout()
        fig_path = run_dir / "figs" / f"pr_{lbl}.png"
        fig.savefig(fig_path, dpi=140)
        plt.close(fig)
    (run_dir / "pr_curves.json").write_text(json.dumps(curves, indent=2), encoding="utf-8")
    return curves

def _classification_report_json(y_true, y_pred, labels, label_names):
    rep = classification_report(y_true, y_pred, labels=labels, target_names=label_names, output_dict=True, zero_division=0)
    return rep

def _write_html_report(run_dir: Path, model_tag: str, metrics: dict, pr_curves: dict, label_map: dict):
    items = []
    for k, v in pr_curves.items():
        ap = v.get("average_precision", 0.0)
        items.append(f'<li>{label_map.get(int(k),k)}: <a href="figs/pr_{k}.png">view</a> (AP={ap:.3f})</li>')
    items_html = "\n".join(items)
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Classification Report - {model_tag}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .metric {{ display: inline-block; margin-right: 12px; padding: 6px 10px; background: #f4f4f4; border-radius: 8px; }}
  </style>
</head>
<body>
  <h1>Classification Report - {model_tag}</h1>
  <div>
    <span class="metric"><b>macro_f1</b>: {metrics.get("macro_f1","n/a"):.4f}</span>
    <span class="metric"><b>brier</b>: {metrics.get("brier","n/a"):.4f}</span>
    <span class="metric"><b>ece</b>: {metrics.get("ece","n/a"):.4f}</span>
    <span class="metric"><b>p1_recall</b>: {metrics.get("p1_recall","n/a"):.4f}</span>
  </div>
  <h3>Confusion Matrix</h3>
  <img src="figs/confusion_matrix.png" alt="Confusion Matrix" />
  <h3>Per-class PR Curves</h3>
  <ul>
    {items_html}
  </ul>
  <p style="color:#666; font-size:12px">Artifacts: classification_report.json, confusion_matrix.json, pr_curves.json, label_map.json, metrics.json</p>
</body>
</html>"""
    (run_dir / "report.html").write_text(html, encoding="utf-8")

def evaluate_and_save(run_dir: Path, y_true, y_pred, labels, label_names, probs=None, model_tag="MODEL"):
    # labels = np.unique(y_true)
    # label_names = label_names or [str(l) for l in labels]

    from sklearn.metrics import f1_score

    # Label map: write if not exists
    lm_path = run_dir / "label_map.json"
    if not lm_path.exists():
        lm = save_label_map(run_dir, labels, label_names)
    else:
        lm = json.loads((run_dir/"label_map.json").read_text())
        
    macro_f1 = float(f1_score(y_true, y_pred, average="macro"))
    name_to_idx = {v: int(k) for k, v in lm.items()}
    p1_idx = name_to_idx["P1"]

    p1_recall = float(((y_true==p1_idx) & (y_pred==p1_idx)).sum() / max(1, (y_true==p1_idx).sum())) if p1_idx in labels else float("nan")

    brier = None; ece = None
    if probs is not None:
        brier = _brier_multiclass(probs, y_true)
        ece = _compute_ece(probs, y_true, n_bins=15)

    metrics = {"macro_f1": macro_f1, "p1_recall": p1_recall}
    if brier is not None: metrics["brier"] = brier
    if ece is not None: metrics["ece"] = ece

    # Report JSON
    clf_rep = _classification_report_json(y_true, y_pred, labels, label_names)
    (run_dir / "classification_report.json").write_text(json.dumps(clf_rep, indent=2), encoding="utf-8")

    # Confusion + PR curves
    cm_json, cm_fig = _save_confusion(run_dir, y_true, y_pred, labels, label_names)
    pr_json = {}
    if probs is not None:
        pr_json = _save_pr_curves(run_dir, y_true, probs, labels, label_names)


    # Save metrics.json
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    # HTML
    # re-read label_map in case it existed
    label_map = json.loads(lm_path.read_text(encoding="utf-8"))
    _write_html_report(run_dir, model_tag, metrics, pr_json, label_map)
    print("Wrote Classification reports to: ", run_dir)

    return metrics
