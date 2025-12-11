import matplotlib.pyplot as plt
import numpy as np

def plot_compare_ig(feature_names, ig_pred, ig_true, pred_class, true_class):
    width = 0.35
    idx = np.arange(len(feature_names))

    plt.figure(figsize=(14,6))
    plt.title(f"Integrated Gradients — Predicted vs True Class\nPred={pred_class}, True={true_class}")

    plt.bar(idx - width/2, ig_pred, width, label=f"Predicted (class {pred_class})", alpha=0.7)
    if ig_true is not None:
        plt.bar(idx + width/2, ig_true, width, label=f"True (class {true_class})", alpha=0.7)

    plt.xticks(idx, feature_names, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.show()
