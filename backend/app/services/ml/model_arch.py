from __future__ import annotations

from typing import Tuple

import torch
from torch import nn


class LSTMAttentionModel(nn.Module):
    def __init__(self, input_size: int, hidden_size: int = 128, num_layers: int = 2, dropout: float = 0.2) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
        )
        self.attn = nn.Linear(hidden_size, 1)
        self.direction_head = nn.Linear(hidden_size, 3)
        self.magnitude_head = nn.Linear(hidden_size, 1)
        self.quality_head = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        seq_out, _ = self.lstm(x)
        logits = self.attn(seq_out).squeeze(-1)
        weights = torch.softmax(logits, dim=1).unsqueeze(-1)
        context = torch.sum(seq_out * weights, dim=1)
        direction_logits = self.direction_head(context)
        magnitude = self.magnitude_head(context).squeeze(-1)
        quality_logits = self.quality_head(context).squeeze(-1)
        return direction_logits, magnitude, quality_logits


class _CausalConvBlock(nn.Module):
    def __init__(self, channels: int, kernel_size: int, dilation: int, dropout: float) -> None:
        super().__init__()
        pad = (kernel_size - 1) * dilation
        self.pad = pad
        self.conv1 = nn.Conv1d(channels, channels, kernel_size=kernel_size, dilation=dilation, padding=pad)
        self.conv2 = nn.Conv1d(channels, channels, kernel_size=kernel_size, dilation=dilation, padding=pad)
        self.act = nn.ReLU()
        self.dropout = nn.Dropout(dropout)

    def _trim(self, x: torch.Tensor) -> torch.Tensor:
        return x[:, :, :-self.pad] if self.pad > 0 else x

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        y = self._trim(self.conv1(x))
        y = self.dropout(self.act(y))
        y = self._trim(self.conv2(y))
        y = self.dropout(self.act(y))
        return self.act(x + y)


class SmallTCNModel(nn.Module):
    def __init__(self, input_size: int, hidden_channels: int = 64, dropout: float = 0.2, kernel_size: int = 3) -> None:
        super().__init__()
        self.proj = nn.Conv1d(input_size, hidden_channels, kernel_size=1)
        self.blocks = nn.ModuleList(
            [_CausalConvBlock(hidden_channels, kernel_size=kernel_size, dilation=d, dropout=dropout) for d in (1, 2, 4, 8)]
        )
        self.direction_head = nn.Linear(hidden_channels, 3)
        self.magnitude_head = nn.Linear(hidden_channels, 1)
        self.quality_head = nn.Linear(hidden_channels, 1)

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        # x: [B, T, F] -> [B, F, T]
        y = self.proj(x.transpose(1, 2))
        for blk in self.blocks:
            y = blk(y)
        context = y[:, :, -1]
        direction_logits = self.direction_head(context)
        magnitude = self.magnitude_head(context).squeeze(-1)
        quality_logits = self.quality_head(context).squeeze(-1)
        return direction_logits, magnitude, quality_logits


class SmallTransformerEncoderModel(nn.Module):
    def __init__(
        self,
        input_size: int,
        hidden_size: int = 64,
        num_layers: int = 2,
        num_heads: int = 4,
        ff_size: int = 128,
        dropout: float = 0.2,
        max_len: int = 512,
    ) -> None:
        super().__init__()
        self.input_proj = nn.Linear(input_size, hidden_size)
        self.pos_emb = nn.Parameter(torch.zeros(1, max_len, hidden_size))
        enc = nn.TransformerEncoderLayer(
            d_model=hidden_size,
            nhead=num_heads,
            dim_feedforward=ff_size,
            dropout=dropout,
            batch_first=True,
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(enc, num_layers=num_layers)
        self.norm = nn.LayerNorm(hidden_size)
        self.direction_head = nn.Linear(hidden_size, 3)
        self.magnitude_head = nn.Linear(hidden_size, 1)
        self.quality_head = nn.Linear(hidden_size, 1)
        nn.init.normal_(self.pos_emb, mean=0.0, std=0.02)

    def _causal_mask(self, t: int, device: torch.device) -> torch.Tensor:
        return torch.triu(torch.ones(t, t, device=device, dtype=torch.bool), diagonal=1)

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        b, t, _ = x.shape
        y = self.input_proj(x)
        pe = self.pos_emb[:, :t, :]
        y = y + pe
        y = self.encoder(y, mask=self._causal_mask(t, y.device))
        context = self.norm(y[:, -1, :])
        direction_logits = self.direction_head(context)
        magnitude = self.magnitude_head(context).squeeze(-1)
        quality_logits = self.quality_head(context).squeeze(-1)
        return direction_logits, magnitude, quality_logits
