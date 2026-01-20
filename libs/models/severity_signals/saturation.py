import re
from collections import defaultdict

def normalize_signal(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text

def extract_bullets(text: str):
    bullets = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("-") or line.startswith("•"):
            bullets.append(line.lstrip("-• ").strip())
    return bullets

class SaturationTracker:
    def __init__(self, time_buckets: list[str], severity_levels: list[str],
                 min_batches_per_bucket: int = 2, min_batches_per_severity: int = 2,
                 window: int = 10, threshold: float = 0.05):    
        """
        time_buckets: list of all coarse time bucket names (strings)
        severity_levels: all severity labels (e.g., ["S1", "S2", "S3", "S4"])
        min_batches_per_bucket: minimum number of batches required per bucket before stopping allowed
        min_batches_per_severity: minimum batches required per severity level
        window: number of recent batches to consider
        threshold: fraction of new signals considered 'low'
        """
        self.time_buckets = set(time_buckets)
        self.severity_levels = set(severity_levels)
        self.min_batches_per_bucket = min_batches_per_bucket
        self.min_batches_per_severity = min_batches_per_severity
        self.window = window
        self.threshold = threshold
        self.seen_signals = set()
        self.history = []
        self.bucket_batch_counts = defaultdict(int)
        self.severity_batch_counts = defaultdict(int)

    def update(self, llm_response: str, batch_time_buckets: set[str], batch_severities: set[str]) -> bool:
        """
        Update tracker with a new LLM response.

        batch_time_buckets: Set of time bucket labels represented in this batch

        Returns True if saturation detected -> to stop signal mining.
        """
        # Track coverage
        for b in batch_time_buckets:
            self.bucket_batch_counts[b] += 1

        for sev in batch_severities:
            self.severity_batch_counts[sev] += 1

        # Extract and normalize signals
        bullets = extract_bullets(llm_response)
        new = 0

        for b in bullets:
            norm = normalize_signal(b)
            if norm not in self.seen_signals:
                self.seen_signals.add(norm)
                new += 1

        self.history.append(new)

        # Check if enough bactches were covered for each time bucket/period
        time_coverage_complete = all(
            self.bucket_batch_counts[b] >= self.min_batches_per_bucket
            for b in self.time_buckets
        )

        if not time_coverage_complete:
            return False  # cannot stop yet
        
        # Check if enough bactches were covered for each severity level
        severity_coverage_complete = all(
            self.severity_batch_counts[s] >= self.min_batches_per_severity
            for s in self.severity_levels
        )

        if not severity_coverage_complete:
            return False
        
        # Check saturation status of signal mining
        if len(self.history) < self.window:
            return False

        recent = self.history[-self.window:]
        avg_new = sum(recent) / max(1, len(self.seen_signals))

        signal_saturated = avg_new < self.threshold

        return signal_saturated
