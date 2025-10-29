
import torch
import torch.nn.functional as F

__all__ = ["fit_temperature", "apply_temperature"]

def fit_temperature(val_logits: torch.Tensor, val_labels: torch.Tensor, init_T: float = 1.0) -> torch.Tensor:
    """Fit a single temperature scalar T (>0) on validation to minimize NLL."""
    T = torch.nn.Parameter(torch.tensor([init_T], dtype=val_logits.dtype, device=val_logits.device))
    opt = torch.optim.LBFGS([T], lr=0.1, max_iter=50, line_search_fn='strong_wolfe')
    def closure():
        opt.zero_grad(set_to_none=True)
        loss = F.cross_entropy(val_logits / T.clamp_min(1e-3), val_labels)
        loss.backward()
        return loss
    opt.step(closure)
    return T.detach().clamp_min(1e-3)

@torch.no_grad()
def apply_temperature(logits: torch.Tensor, T: torch.Tensor) -> torch.Tensor:
    return (logits / T).softmax(dim=1)
