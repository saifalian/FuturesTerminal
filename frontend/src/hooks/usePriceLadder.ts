import { useMemo } from "react";
import { LadderSnapshot } from "../types/market";

function formatLiquidity(value: number) {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(1);
}

export function usePriceLadder(ladder: LadderSnapshot | null) {
  return useMemo(() => {
    if (!ladder) return [];
    const maxValue = Math.max(1, ...ladder.rows.map((row) => Math.log1p(row.liquidity)));
    return ladder.rows.map((row) => {
      const intensity = Math.max(0, Math.min(1, Math.log1p(row.liquidity) / maxValue));
      return {
        ...row,
        intensity,
        formatted: formatLiquidity(row.liquidity),
      };
    });
  }, [ladder]);
}
