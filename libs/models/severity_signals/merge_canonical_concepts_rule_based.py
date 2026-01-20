

from collections import defaultdict
from pathlib import Path
import json


import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WORKING_DIR = Path("artifacts")

CONCEPT_FILE = WORKING_DIR / "severity_signals" / "concepts.jsonl"
MERGED_CONCEPT_FILE = WORKING_DIR / "severity_signals" / "merged_concepts.jsonl"
CONCEPT_MERGE_MAP_FILE = WORKING_DIR / "severity_signals" / "concept_merge_map.jsonl"

MERGE_RULES = {
    "core" : {
        "stability_crashes": [
            "crashes_and_stability_issues",
            "component_failure",
            "shutdown_issues",
            "blocking_operations",  
        ],

        "performance_and_resource_issues": [
            "performance_degradation",
            "performance_delays",
            "resource_management_concerns",
            "inefficient_algorithms",
        ],

        "security_and_data_exposure": [
            "security_vulnerabilities",
            "user_data_exposure",
            "data_integrity_risk",
            "file_integrity_and_naming",
            "crossorigin_issues",
        ],

        "functional_correctness_errors": [
            "functional_failures",
            "algorithmic_errors",
            "error_handling_omissions",
        ],

        "build_and_integration_failures": [
            "warnings_and_compiler_issues",
            "integration_challenges",
            "interaction_with_javascript",
            "crosscomponent_interaction",
        ],

        "state_and_session_failures": [
            "session_management",
            "user_authentication_failure",
        ],

        "regression_from_previous_behavior": [
            "regression_indication",
        ],
    },
    "impact": {
        # IMPACT
        "user_experience_degradation": [
            "user_experience_disruption",
            "impact_on_user_experience",
            "user_confusion_potential",
            "user_expectations",
            "uiux_adaptation",
        ],

        "user_interaction_blockage": [
            "user_interaction_blockage",
            "modal_dialog_behavior",
            "focus_management",
            "clipboard_functionality_failures",
        ],

        "visual_and_accessibility_issues": [
            "visibility_and_accessibility_problems",
            "character_rendering_issues",
            "color_management_failures",
            "user_interface_changes",
        ],

        "user_context_specific_impact": [
            "useragent_specificity",
            "user_context_and_segregation",
            "user_base_size",
        ],
    },
}

CONTEXT_CONCEPT = [
    "reproducibility",
    "specificity_of_conditions",
    "platformspecific_issues",
    "crossplatform_compatibility_issues",
    "intermittent_test_failures",
    "environmental_factors",
    "complexity_of_use_cases",
]

def merge_concepts(canonical_concepts, merge_rules):
    name_to_concept = {c["name"]: c for c in canonical_concepts}

    merged_concepts = []
    merge_map = []

    # Merge for CORE and IMPACT concepts based on merge rules
    for role_name, role in merge_rules.items():
        for merged_name, member_names in role.items():
            merged_id = f"MC_{merged_name}"

            member_concept_ids = []
            member_signal_ids = set()
            evidence = []

            for name in member_names:
                if name not in name_to_concept:
                    continue

                c = name_to_concept[name]
                member_concept_ids.append(c["concept_id"])
                member_signal_ids.update(c["member_signal_ids"])

                # sample evidence (avoid explosion)
                evidence.extend(c["explanation_evidence"][:3])

                merge_map.append({
                    "concept_id": c["concept_id"],
                    "merged_id": merged_id,
                    "merged_name": merged_name,
                })

            merged_concepts.append({
                "merged_id": merged_id,
                "merged_name": merged_name,
                "member_concept_ids": member_concept_ids,
                "member_signal_ids": sorted(member_signal_ids),
                "evidence": evidence[:10],  # cap for sanity
                "role": role_name,
            })

    # Add (keep as-is, not merge any) selected CONTEXT concepts to final merged concepts list
    for ctx_name in CONTEXT_CONCEPT:
        ctx_id = f"CTX_{ctx_name}"
        member_concept_ids = []
        member_signal_ids = []
        evidence = []

        ctx_c = name_to_concept[ctx_name]
        member_concept_ids.append(ctx_c["concept_id"])
        member_signal_ids.extend(ctx_c["member_signal_ids"])
        evidence.extend(ctx_c["explanation_evidence"][:3])

        merge_map.append({
            "concept_id": ctx_c["concept_id"],
            "merged_id": ctx_id,
            "merged_name": ctx_name,
        })

        merged_concepts.append({
            "merged_id": ctx_id,
            "merged_name": ctx_name,
            "member_concept_ids": member_concept_ids,
            "member_signal_ids": sorted(member_signal_ids),
            "evidence": evidence[:10],  # cap for sanity
            "role": "context"
        })

    return merged_concepts, merge_map


if __name__ == "__main__":
    canonical_concepts = []

    with open(CONCEPT_FILE, "r") as f:
        for line in f:
            canonical_concepts.append(json.loads(line))

    merged_concepts, concept_merge_map = merge_concepts(
        canonical_concepts,
        MERGE_RULES 
    )

    for m in merged_concepts:
        print("\n", m)

    # Ensure all canonical concepts are accounted for
    covered = {m["concept_id"] for m in concept_merge_map}
    all_ids = {c["concept_id"] for c in canonical_concepts}

    missing = all_ids - covered
    print("Unmerged concept_ids:", missing)

    # Save merged concepts and merge map
    with open(MERGED_CONCEPT_FILE, "w", encoding="utf-8") as fout:
        for merged in merged_concepts:
            fout.write(json.dumps(merged) + "\n")
        
    with open(CONCEPT_MERGE_MAP_FILE, "w", encoding="utf-8") as fout:
        for map in concept_merge_map:
            fout.write(json.dumps(map) + "\n")



