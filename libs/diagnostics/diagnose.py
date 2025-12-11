"""
Call funcitons in libs/diagnostics/full_diagnostics.py 
"""

import json
import numpy as np
import torch
from pathlib import Path
import pandas as pd
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.diagnostics.full_diagnostics import run_full_diagnostics


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
PREP_ID = "pca225_tv0.95_sc103261f6_nt95_fmB1"
RUN_ID = "clf_mlp_B1_20251210_233115"
MODEL_ROOT = f"artifacts/models/severity/{PREP_ID}/{RUN_ID}"
MODEL_PATH = f"artifacts/models/severity/{PREP_ID}/{RUN_ID}/best_model.pt"

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"


model = torch.load(MODEL_PATH, map_location=DEVICE).to(DEVICE)
run_full_diagnostics(model, PREP_ID, RUN_ID, device=DEVICE, ordinal=True)

