import numpy as np
from collections import Counter, defaultdict
from scipy.stats import entropy
from sklearn.metrics import mutual_info_score


def compute_topic_label_distribution(topic_ids, labels):
    """
    Returns: dict topic_id -> {class -> count}
    """
    dist = defaultdict(lambda: Counter())

    for t, y in zip(topic_ids, labels):
        dist[t][y] += 1

    return dist


def topic_purity(dist):
    """
    Purity = max_class_count / total_count for each topic.
    Returns dict topic_id -> purity score.
    """
    purity_dict = {}

    for t, counter in dist.items():
        total = sum(counter.values())
        if total == 0:
            purity_dict[t] = 0
            continue

        majority = max(counter.values())
        purity_dict[t] = majority / total

    return purity_dict


def topic_entropy(dist):
    """
    Shannon entropy of label distribution per topic.
    Lower = better (more pure)
    """
    entropy_dict = {}

    for t, counter in dist.items():
        counts = np.array(list(counter.values()))
        p = counts / counts.sum()
        entropy_dict[t] = float(entropy(p, base=2))

    return entropy_dict


def topic_kl_uniform(dist):
    """
    KL divergence from uniform distribution.
    Higher = more pure topic; lower = uninformative topic.
    """
    kl_dict = {}
    for t, counter in dist.items():
        counts = np.array(list(counter.values()), dtype=float)
        p = counts / counts.sum()

        K = len(counts)
        u = np.ones(K) / K

        kl = np.sum(p * np.log(p / u + 1e-10))
        kl_dict[t] = float(kl)

    return kl_dict


def global_mutual_information(topic_ids, labels):
    """
    MI(topic ; severity)
    Higher = better topic-label alignment.
    """
    return float(mutual_info_score(topic_ids, labels))


def compute_topic_level_ig(records, topic_block_span):
    """
    Aggregates IG importance for topic features across dataset.
    """
    s, e = topic_block_span
    all_igs = [rec["ig_pred"][s:e] for rec in records]
    return np.mean(np.stack(all_igs), axis=0)

