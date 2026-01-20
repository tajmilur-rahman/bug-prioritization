"""
This step does the followning:
- Remove noise to embedding like: Code block, stacktrace, file path, hex address, line numbers.
- Use regex + semantic structuring to help:
    - inject domain knowledge
    - stabilize embeddings
    - preserve information
    - improve learnability
    For each bug report, this step produces below:
    - Issue type:
    - Observed behavior:
    - Impact:
    - Conditions:
    - Technical signals:
    Above information together with bugs' texts are input to embedding step.

Notes:
We are using all-MiniLM-L6-v2, which is trained for sentence similarity, not analysis. 
Then semantic signal competes with: irrelevant tokens, rare identifiers, accidental co-occurrences.
Regex + structure helps: improve signals, make semantic dimensions linearly accessible, 
and reduce embedding variance. This directly improves downstream learning.

In case we use an LLM, which has strong reasoning capacity, for embedding, we will not need the regex + semantic injection part.
"""
import re
from typing import Dict, Set, List

# =========================================================
# Regex & heuristics for noise normalization (NOT deletion)
# =========================================================

URL_RE = re.compile(r"https?://[^\s\)\]\}<>]+", re.IGNORECASE)
HEX_ADDR_RE = re.compile(r"\b0x[0-9a-fA-F]+\b")
LINE_NUM_RE = re.compile(r"\bline\s+\d+\b", re.IGNORECASE)
FILE_PATH_RE = re.compile(r"(/[\w\-/\.]+)|([A-Za-z]:\\[\w\\\.]+)")

STACK_SIGNAL_RE = re.compile(
    r"(SIGSEGV|Segmentation fault|Assertion failure|stack trace|backtrace|panic)",
    re.IGNORECASE,
)

BOILERPLATE_RE = re.compile(
    r"(Randomly chosen test:.*?\n){2,}",
    flags=re.IGNORECASE | re.DOTALL,
)

# =========================================================
# Technical signal extraction 
# =========================================================

TECH_SIGNAL_PATTERNS = {
    "out_of_memory": r"\b(OOM|out of memory|memory leak)\b",
    "assertion_failure": r"\bassert(ion)?\b",
    "segmentation_fault": r"\b(segfault|segmentation fault)\b",
    "null_pointer": r"\bnull pointer\b",
    "deadlock": r"\bdeadlock\b",
    "race_condition": r"\brace condition\b",
    "stack_trace_present": r"\b(stack trace|backtrace)\b",
    "crash": r"\b(crash|crashes|crashed)\b",
    "hang_or_freeze": r"\b(hang|freeze|unresponsive)\b",
    "performance_issue": r"\b(slow|latency|performance degradation)\b",
    "security_issue": r"\b(vulnerability|exploit|security)\b",
    "regression": r"\bregression\b",
    "test_failure": r"\b(test failure|test failed|flaky test)\b",
}

# =========================================================
# Impact inference 
# =========================================================

IMPACT_MAP = {
    "security_issue": "security or privacy risk",
    "crash": "loss of functionality or data",
    "out_of_memory": "system instability or crash risk",
    "hang_or_freeze": "loss of responsiveness",
    "performance_issue": "degraded performance or usability",
    "test_failure": "reduced reliability or confidence",
}

# =========================================================
# Condition inference 
# =========================================================

CONDITION_PATTERNS = {
    "platform_specific": r"\b(windows|linux|mac|android|ios)\b",
    "version_specific": r"\b(version|build|release)\b",
    "reproducible": r"\b(always|consistently|reproducible)\b",
    "intermittent": r"\b(intermittent|sometimes|sporadic)\b",
    "configuration_specific": r"\b(config|setting|preference)\b",
}

# =========================================================
# Utilities
# =========================================================

def extract_matches(patterns: Dict[str, str], text: str) -> Set[str]:
    found = set()
    for name, pattern in patterns.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.add(name)
    return found


# =========================================================
# Code-like line detection (heuristic, NOT regex-only)
# =========================================================

def is_code_like(line: str) -> bool:
    line = line.strip()
    if len(line) < 10:
        return False
    return (
        line.count("{") + line.count("}") >= 2
        or (";" in line and "(" in line and ")" in line)
        or line.startswith(("function ", "var ", "let ", "const "))
        or line.startswith(("for ", "while ", "if "))
        or line.startswith(("#", "//"))
        or ("=" in line and "(" in line)
    )


def collapse_code_blocks(text: str, max_lines: int = 5) -> str:
    lines = text.splitlines()
    out = []
    code_run = 0

    for line in lines:
        if is_code_like(line):
            code_run += 1
            if code_run == 1:
                out.append("<CODE_BLOCK>")
            if code_run >= max_lines:
                continue
        else:
            code_run = 0
            out.append(line)

    return "\n".join(out)


# =========================================================
# Core noise normalization 
# =========================================================

def normalize_noise(text: str) -> str:
    if not text:
        return ""

    text = URL_RE.sub("<URL>", text)
    text = FILE_PATH_RE.sub("<FILE_PATH>", text)
    text = HEX_ADDR_RE.sub("<HEX>", text)
    text = LINE_NUM_RE.sub(" ", text)

    text = BOILERPLATE_RE.sub("<TEST_BOILERPLATE>\n", text)
    text = collapse_code_blocks(text)

    if STACK_SIGNAL_RE.search(text):
        text += "\n<STACKTRACE_PRESENT>"

    text = re.sub(r"\s+", " ", text)
    return text.strip()


# =========================================================
# Semantic structuring for embedding (FINAL)
# =========================================================

def build_embedding_text(raw_text: str) -> str:
    """
    raw_text should include summary/title + description
    Output is optimized for MiniLM embeddings
    """

    if not raw_text:
        return ""

    clean_body = normalize_noise(raw_text)

    technical_signals = extract_matches(TECH_SIGNAL_PATTERNS, raw_text)
    conditions = extract_matches(CONDITION_PATTERNS, raw_text)

    # -------------------------
    # Issue type (robust, same logic)
    # -------------------------
    issue_types = []
    if {"crash", "hang_or_freeze"} & technical_signals:
        issue_types.append("stability")
    if "performance_issue" in technical_signals:
        issue_types.append("performance")
    if "security_issue" in technical_signals:
        issue_types.append("security")
    if "regression" in technical_signals:
        issue_types.append("regression")
    if "test_failure" in technical_signals:
        issue_types.append("testing")

    issue_type_text = ", ".join(issue_types) if issue_types else "general defect"

    # -------------------------
    # Impact 
    # -------------------------
    impacts = {
        IMPACT_MAP[s]
        for s in technical_signals
        if s in IMPACT_MAP
    }

    # -------------------------
    # Assemble final embedding text
    # -------------------------
    sections: List[str] = []

    sections.append(f"Issue type:\n{issue_type_text}")

    if clean_body:
        sections.append(f"Observed behavior:\n{clean_body[:2500]}")

    if impacts:
        sections.append(f"Impact:\n{', '.join(sorted(impacts))}")

    if conditions:
        sections.append(f"Conditions:\n{', '.join(sorted(conditions))}")

    if technical_signals:
        sections.append(
            f"Technical signals:\n{', '.join(sorted(technical_signals))}"
        )

    return "\n\n".join(sections)
