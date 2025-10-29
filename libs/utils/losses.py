
import torch
import torch.nn as nn
import torch.nn.functional as F

__all__ = ["WeightedCrossEntropy", "FocalLoss"]

class WeightedCrossEntropy(nn.Module):
    def __init__(self, class_weights=None):
        super().__init__()
        if class_weights is None:
            self.register_buffer("weights", None)
        else:
            self.register_buffer("weights", torch.as_tensor(class_weights, dtype=torch.float).detach())
    def forward(self, logits, targets):
        return F.cross_entropy(logits, targets, weight=self.weights)

class FocalLoss(nn.Module):
    def __init__(self, gamma=1.5, alpha=None, reduction="mean"):
        super().__init__()
        self.gamma = gamma
        self.reduction = reduction
        if alpha is None:
            self.register_buffer("alpha", None)
        else:
            self.register_buffer("alpha", torch.tensor(alpha, dtype=torch.float).detach())
    def forward(self, logits, targets):
        logp = F.log_softmax(logits, dim=1)
        p = logp.exp()
        pt = p[torch.arange(logits.size(0)), targets]
        focal = (1 - pt) ** self.gamma
        loss = -focal * logp[torch.arange(logits.size(0)), targets]
        if self.alpha is not None:
            at = self.alpha[targets]
            loss = loss * at
        if self.reduction == "mean":
            return loss.mean()
        elif self.reduction == "sum":
            return loss.sum()
        return loss
