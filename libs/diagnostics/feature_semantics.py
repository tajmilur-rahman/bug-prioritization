"""
feature_semantics.py — resolves feature indexes into readable names.
"""

def build_feature_semantics(schema):
    """
    Returns:
        feature_names: list[str] length = total_dim
    """

    names = []
    for block in schema["used_blocks"]:
        width = schema["blocks"][block]

        if block.startswith("text_pca"):
            names.extend([f"text_pca_{i}" for i in range(width)])

        elif block.startswith("topic_pca"):
            names.extend([f"topic_pca_{i}" for i in range(width)])

        elif block == "categorical":
            cats = schema.get("categorical_cols", [])
            dims_per_col = width // max(1, len(cats))
            for c in cats:
                for j in range(dims_per_col):
                    names.append(f"cat:{c}:{j}")

        elif block == "numeric":
            nums = schema.get("numeric_cols", [])
            assert len(nums) == width
            for c in nums:
                names.append(f"num:{c}")

        elif "sim" in block.lower():
            names.extend([f"sim:{i}" for i in range(width)])

        else:
            names.extend([f"{block}_{i}" for i in range(width)])

    return names


def resolve_index(idx, schema):
    """
    Returns: {"block": "...", "offset": int, "name": "..."}
    """
    names = build_feature_semantics(schema)
    return {
        "index": idx,
        "name": names[idx],
    }

