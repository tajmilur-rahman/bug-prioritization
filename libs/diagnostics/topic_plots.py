import numpy as np
import matplotlib.pyplot as plt


def plot_topic_purity(purity_dict):
    topics = list(purity_dict.keys())
    vals = [purity_dict[t] for t in topics]

    plt.figure(figsize=(10,4))
    plt.bar(topics, vals)
    plt.title("Topic Purity (Majority Class Fraction)")
    plt.xlabel("Topic ID")
    plt.ylabel("Purity")
    plt.show()


def plot_topic_entropy(ent_dict):
    topics = list(ent_dict.keys())
    vals = [ent_dict[t] for t in topics]

    plt.figure(figsize=(10,4))
    plt.bar(topics, vals)
    plt.title("Topic Label Entropy")
    plt.xlabel("Topic ID")
    plt.ylabel("Entropy (bits)")
    plt.show()


def plot_topic_confusion_heatmap(dist):
    """
    dist: topic_id -> counter(class)
    Displays topic vs class distribution heatmap.
    """
    unique_topics = sorted(dist.keys())
    classes = sorted({c for d in dist.values() for c in d})

    mat = np.zeros((len(unique_topics), len(classes)))

    for i, t in enumerate(unique_topics):
        counter = dist[t]
        for j, c in enumerate(classes):
            mat[i, j] = counter[c]

    plt.figure(figsize=(10,7))
    plt.imshow(mat, cmap="viridis")
    plt.colorbar(label="Count")
    plt.xticks(np.arange(len(classes)), classes)
    plt.yticks(np.arange(len(unique_topics)), unique_topics)
    plt.xlabel("Severity Class")
    plt.ylabel("Topic ID")
    plt.title("Topic-Severity Heatmap")
    plt.show()
