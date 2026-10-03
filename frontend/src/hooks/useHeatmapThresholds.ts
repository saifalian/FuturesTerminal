import { useMemo, useState } from "react";
import { HeatmapThresholdState } from "../types/market";

export function useHeatmapThresholds() {
  const [state, setState] = useState<HeatmapThresholdState>({ low: 0.05, high: 0.9 });

  const safe = useMemo(() => {
    const low = Math.max(0, Math.min(0.95, state.low));
    const high = Math.max(low + 0.01, Math.min(1, state.high));
    return { low, high };
  }, [state.high, state.low]);

  function setLow(value: number) {
    setState((prev) => ({ ...prev, low: value }));
  }

  function setHigh(value: number) {
    setState((prev) => ({ ...prev, high: value }));
  }

  function reset() {
    setState({ low: 0.05, high: 0.9 });
  }

  return {
    low: safe.low,
    high: safe.high,
    setLow,
    setHigh,
    reset,
  };
}
