"""
losses.py — Production-grade unified loss functions for Bug Severity/Priority models.

Includes:
  - Weighted Cross Entropy
  - Focal Loss
  - Class-Balanced Focal Loss (CB Loss)
  - CORAL Ordinal Loss (for Hybrid Softmax+Ordinal MLP)
  - LossFactory for clean trainer integration
"""

import torch
import torch.nn as nn
import torch.nn.functional as F

__all__ = [
    "WeightedCrossEntropy",
    "FocalLoss",
    "ClassBalancedFocalLoss",
    "CORALOrdinalLoss",
    "LossFactory"
]

# -----------------------------------------------------------
# 1. Weighted Cross Entropy
# -----------------------------------------------------------
class WeightedCrossEntropy(nn.Module):
    def __init__(self, class_weights=None):
        super().__init__()
        if class_weights is None:
            self.register_buffer("weights", None)
        else:
            self.register_buffer("weights", torch.as_tensor(class_weights, dtype=torch.float32))

    def forward(self, logits, targets):
        return F.cross_entropy(logits, targets, weight=self.weights)


# -----------------------------------------------------------
# 2. Focal Loss (improved version using gather)
# -----------------------------------------------------------
class FocalLoss(nn.Module):
    def __init__(self, gamma=1.5, alpha=None, reduction="mean"):
        super().__init__()
        self.gamma = gamma
        self.reduction = reduction
        if alpha is None:
            self.register_buffer("alpha", None)
        else:
            self.register_buffer("alpha", torch.tensor(alpha, dtype=torch.float32))

    def forward(self, logits, targets):
        log_probs = F.log_softmax(logits, dim=1)         # (N, C)
        probs = torch.exp(log_probs)

        # gather target probabilities
        pt = probs.gather(1, targets.unsqueeze(1)).squeeze(1)     # (N,)

        focal_weight = (1 - pt) ** self.gamma
        loss = -focal_weight * log_probs.gather(1, targets.unsqueeze(1)).squeeze(1)

        if self.alpha is not None:
            alpha_t = self.alpha[targets]
            loss = loss * alpha_t

        if self.reduction == "mean":
            return loss.mean()
        if self.reduction == "sum":
            return loss.sum()
        return loss


# -----------------------------------------------------------
# 3. Class-Balanced Focal Loss (Cui et al. 2019)
# -----------------------------------------------------------
class ClassBalancedFocalLoss(nn.Module):
    """
    Implements: CB Loss = (1 - β) / (1 - β^n) * FocalLoss
    where n = class count
    """
    def __init__(self, class_counts, beta=0.999, gamma=1.5):
        super().__init__()
        effective_num = 1.0 - (beta ** torch.tensor(class_counts, dtype=torch.float32))
        weights = (1.0 - beta) / effective_num
        weights = weights / weights.sum()     # normalize

        self.register_buffer("weights", weights)
        self.gamma = gamma

    def forward(self, logits, targets):
        log_probs = F.log_softmax(logits, dim=1)
        probs = torch.exp(log_probs)

        pt = probs.gather(1, targets.unsqueeze(1)).squeeze(1)
        focal = (1 - pt) ** self.gamma

        ce = -log_probs.gather(1, targets.unsqueeze(1)).squeeze(1)
        loss = focal * ce * self.weights[targets]

        return loss.mean()


# -----------------------------------------------------------
# 4. CORAL Ordinal Loss (needed for Hybrid MLP)
# -----------------------------------------------------------
class CORALOrdinalLoss(nn.Module):
    """
    CORAL (Consistent Rank Logits):
      - logits shape = (N, K-1)
      - targets shape = (N,) values in [0..K-1]
    """
    def forward(self, logits, targets):
        # logits: (N, K-1)
        # transform target to binary rank representation
        # e.g. class 2 of 4 → [1, 1, 0]
        num_levels = logits.size(1)
        # Build cumulative labels
        y = torch.zeros((targets.size(0), num_levels), device=logits.device)
        for k in range(num_levels):
            y[:, k] = (targets > k).float()

        return F.binary_cross_entropy_with_logits(logits, y, reduction="mean")


# -----------------------------------------------------------
# 5. LossFactory (central entry point for trainers)
# -----------------------------------------------------------
class LossFactory:
    @staticmethod
    def make(loss_name: str,
             num_classes: int,
             class_counts=None,
             class_weights=None,
             focal_gamma=1.5,
             cb_beta=0.999):
        """
        loss_name ∈ {
            "ce", "weighted_ce", "focal",
            "cb_focal", "coral"
        }
        """
        loss_name = loss_name.lower()

        if loss_name == "ce":
            return WeightedCrossEntropy(None)

        if loss_name == "weighted_ce":
            return WeightedCrossEntropy(class_weights)

        if loss_name == "focal":
            return FocalLoss(gamma=focal_gamma, alpha=class_weights)

        if loss_name == "cb_focal":
            if class_counts is None:
                raise ValueError("Class counts required for CB Loss")
            return ClassBalancedFocalLoss(class_counts, beta=cb_beta, gamma=focal_gamma)

        if loss_name == "coral":
            # K classes → logits dim = K-1
            return CORALOrdinalLoss()

        raise ValueError(f"Unknown loss type: {loss_name}")
