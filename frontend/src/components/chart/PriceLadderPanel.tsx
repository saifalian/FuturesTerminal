import { LadderSnapshot } from "../../types/market";
import { usePriceLadder } from "../../hooks/usePriceLadder";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { ChartHorizontalLine } from "../../types/market";

const BASE_VISIBLE_LADDER_ROWS = 120;

type PriceLadderPanelProps = {
  ladder: LadderSnapshot | null;
  tabId?: string;
  onOverlayLinesChange?: (lines: ChartHorizontalLine[]) => void;
};

function ladderColor(side: "above" | "below" | "at", intensity: number) {
  const alpha = 0.12 + intensity * 0.85;
  // Above current price is ask/sell side, below is bid/buy side.
  if (side === "above") return `rgba(230, 75, 85, ${alpha})`;
  if (side === "below") return `rgba(22, 183, 120, ${alpha})`;
  return `rgba(200, 200, 200, ${alpha})`;
}

function formatStepInput(value: number): string {
  return value.toLocaleString("en-US", {
    useGrouping: false,
    maximumFractionDigits: 12,
  });
}

function stepToPriceDecimals(step: number): number {
  if (!Number.isFinite(step) || step <= 0) return 4;
  const decimals = Math.ceil(Math.max(0, -Math.log10(step)));
  return Math.max(2, Math.min(8, decimals));
}

export default function PriceLadderPanel({ ladder, tabId = "default", onOverlayLinesChange }: PriceLadderPanelProps) {
  const listRef = useRef<HTMLDivElement | null>(null);
  const panelRef = useRef<HTMLElement | null>(null);
  const lastSymbolRef = useRef<string | null>(null);
  const lastCenterSignatureRef = useRef<string>("");
  const userDetachedUntilRef = useRef<number>(0);
  const [isPaused, setIsPaused] = useState(false);
  const [displayRows, setDisplayRows] = useState<ReturnType<typeof usePriceLadder>>([]);
  const [showSettings, setShowSettings] = useState(false);
  const [selectedRowPrice, setSelectedRowPrice] = useState<number | null>(null);
  const [markHighAmountArea, setMarkHighAmountArea] = useState(true);
  const [highAmountRows, setHighAmountRows] = useState(2);
  const [showHighAmountOnChart, setShowHighAmountOnChart] = useState(false);
  const [markSizeThreshold, setMarkSizeThreshold] = useState(false);
  const [sizeThresholdMinAmount, setSizeThresholdMinAmount] = useState(0);
  const [sizeThresholdMaxAmount, setSizeThresholdMaxAmount] = useState(0);
  const [sizeThresholdUnit, setSizeThresholdUnit] = useState<"K" | "M">("K");
  const [showSizeThresholdOnChart, setShowSizeThresholdOnChart] = useState(false);
  const [showSelectedRowOnChart, setShowSelectedRowOnChart] = useState(false);
  const [priceStepPreset, setPriceStepPreset] = useState<"auto" | "1" | "0.1" | "0.01" | "0.001" | "0.0001" | "custom">("auto");
  const [customPriceStep, setCustomPriceStep] = useState(0.01);
  const [customPriceStepInput, setCustomPriceStepInput] = useState("0.01");
  const [ladderRangeMultiplier, setLadderRangeMultiplier] = useState(1);
  const ladderRangeApplyTimer = useRef<number | null>(null);
  // Intuitive: 1x = narrow/default, 10x = widest.
  const effectiveRangeMultiplier = Math.max(1, ladderRangeMultiplier);

  const ladderWithPriceStep = useMemo<LadderSnapshot | null>(() => {
    if (!ladder) return null;
    const baseStep = Math.max(0.00000001, Number(ladder.bucket_size || 0.0001));
    const rawStep =
      priceStepPreset === "auto"
        ? baseStep
        : priceStepPreset === "custom"
          ? customPriceStep
          : Number(priceStepPreset);
    const explicitStep = priceStepPreset !== "auto";
    const selectedStep = explicitStep
      ? Math.max(0.00000001, Number.isFinite(rawStep) ? rawStep : baseStep)
      : baseStep;
    if (!explicitStep && Math.abs(selectedStep - baseStep) < 0.000000001 && effectiveRangeMultiplier <= 1) return ladder;

    const grouped = new Map<number, number>();
    for (const row of ladder.rows) {
      const key = Math.round(row.price / selectedStep);
      const existing = grouped.get(key);
      if (!existing) {
        grouped.set(key, row.liquidity);
      } else {
        grouped.set(key, existing + row.liquidity);
      }
    }

    const visibleRowsTarget = Math.max(20, BASE_VISIBLE_LADDER_ROWS);
    const centerKey = Math.round(ladder.current_price / selectedStep);
    const spanRowsTarget = Math.max(
      visibleRowsTarget,
      Math.floor(visibleRowsTarget * Math.max(1, effectiveRangeMultiplier)),
    );
    const halfSpan = Math.floor(spanRowsTarget / 2);
    const startKey = centerKey - halfSpan;
    const endKey = startKey + spanRowsTarget - 1;
    const halfStep = selectedStep * 0.5;

    const spanRows: Array<{ row: number; price: number; liquidity: number; side: "above" | "below" | "at" }> = [];
    for (let key = endKey; key >= startKey; key -= 1) {
      const price = key * selectedStep;
      const liquidity = grouped.get(key) ?? 0;
      const side = price > ladder.current_price + halfStep ? "above" : price < ladder.current_price - halfStep ? "below" : "at";
      spanRows.push({
        row: key,
        price,
        liquidity,
        side,
      });
    }

    let visibleRows = spanRows;
    if (!explicitStep && spanRows.length > visibleRowsTarget) {
      const bucketSize = spanRows.length / visibleRowsTarget;
      const compressed: typeof spanRows = [];
      for (let i = 0; i < visibleRowsTarget; i += 1) {
        const start = Math.floor(i * bucketSize);
        let end = Math.floor((i + 1) * bucketSize) - 1;
        if (end < start) end = start;
        if (start >= spanRows.length) break;
        if (end >= spanRows.length) end = spanRows.length - 1;
        const segment = spanRows.slice(start, end + 1);
        if (segment.length === 0) continue;
        const representative = segment.reduce((best, item) => (item.liquidity > best.liquidity ? item : best), segment[0]);
        const liquidity = representative.liquidity;
        const side =
          representative.price > ladder.current_price + selectedStep * 0.5
            ? "above"
            : representative.price < ladder.current_price - selectedStep * 0.5
              ? "below"
              : "at";
        compressed.push({
          row: representative.row,
          price: representative.price,
          liquidity,
          side,
        });
      }
      if (compressed.length > 0) {
        visibleRows = compressed;
      }
    }

    return {
      ...ladder,
      bucket_size: selectedStep,
      rows: visibleRows,
    };
  }, [customPriceStep, effectiveRangeMultiplier, ladder, priceStepPreset]);

  useEffect(() => {
    if (ladderRangeApplyTimer.current) {
      window.clearTimeout(ladderRangeApplyTimer.current);
    }
    ladderRangeApplyTimer.current = window.setTimeout(() => {
      fetch("http://127.0.0.1:8000/terminal/ladder/range", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tab_id: tabId, range_multiplier: effectiveRangeMultiplier }),
      }).catch(() => {});
    }, 180);

    return () => {
      if (ladderRangeApplyTimer.current) {
        window.clearTimeout(ladderRangeApplyTimer.current);
        ladderRangeApplyTimer.current = null;
      }
    };
  }, [effectiveRangeMultiplier, tabId]);

  useEffect(() => {
    if (priceStepPreset !== "custom") return;
    setCustomPriceStepInput(formatStepInput(customPriceStep));
  }, [customPriceStep, priceStepPreset]);

  const rows = usePriceLadder(ladderWithPriceStep);
  const priceDecimals = useMemo(
    () => stepToPriceDecimals(Number(ladderWithPriceStep?.bucket_size ?? ladder?.bucket_size ?? 0.0001)),
    [ladder?.bucket_size, ladderWithPriceStep?.bucket_size],
  );
  const selectedTolerance = useMemo(
    () => Math.max(0.0000000001, Number(ladderWithPriceStep?.bucket_size ?? ladder?.bucket_size ?? 0.0001) * 0.5),
    [ladder?.bucket_size, ladderWithPriceStep?.bucket_size],
  );

  function resolveCenterIndex() {
    if (!displayRows.length || !ladderWithPriceStep) return -1;
    let centerIndex = displayRows.findIndex((row) => row.side === "at");
    if (centerIndex < 0) {
      centerIndex = displayRows.reduce((bestIndex, row, index) => {
        const bestDistance = Math.abs(displayRows[bestIndex].price - ladderWithPriceStep.current_price);
        const currentDistance = Math.abs(row.price - ladderWithPriceStep.current_price);
        return currentDistance < bestDistance ? index : bestIndex;
      }, 0);
    }
    return centerIndex;
  }

  function recenterLadder() {
    const container = listRef.current;
    if (!container || displayRows.length === 0 || !ladderWithPriceStep) return;
    const firstRow = container.querySelector<HTMLElement>(".ladder-row");
    if (!firstRow) return;
    const centerIndex = resolveCenterIndex();
    if (centerIndex < 0) return;

    const rowHeight = firstRow.offsetHeight || 24;
    const style = window.getComputedStyle(container);
    const rowGap = Number.parseFloat(style.rowGap || style.gap || "0") || 0;
    const rowStep = rowHeight + rowGap;
    const targetTop = Math.max(0, centerIndex * rowStep - (container.clientHeight - rowHeight) / 2);
    if (Math.abs(container.scrollTop - targetTop) > 0.5) {
      container.scrollTop = targetTop;
    }
  }

  function smoothFollowCenter() {
    const container = listRef.current;
    if (!container || displayRows.length === 0 || !ladderWithPriceStep) return;
    if (isPaused) return;
    if (Date.now() < userDetachedUntilRef.current) return;

    const firstRow = container.querySelector<HTMLElement>(".ladder-row");
    if (!firstRow) return;
    const centerIndex = resolveCenterIndex();
    if (centerIndex < 0) return;

    const rowHeight = firstRow.offsetHeight || 24;
    const style = window.getComputedStyle(container);
    const rowGap = Number.parseFloat(style.rowGap || style.gap || "0") || 0;
    const rowStep = rowHeight + rowGap;
    const targetTop = Math.max(0, centerIndex * rowStep - (container.clientHeight - rowHeight) / 2);
    const currentTop = container.scrollTop;
    const diff = targetTop - currentTop;

    const deadZone = rowStep * 0.35;
    if (Math.abs(diff) <= deadZone) return;

    const step = Math.max(-rowStep * 1.5, Math.min(rowStep * 1.5, diff * 0.18));
    if (Math.abs(step) < 0.2) return;
    container.scrollTop = currentTop + step;
  }

  useEffect(() => {
    if (isPaused) return;
    setDisplayRows(rows);
  }, [isPaused, rows]);

  useEffect(() => {
    const symbol = ladderWithPriceStep?.symbol ?? null;
    if (lastSymbolRef.current !== symbol) {
      lastSymbolRef.current = symbol;
      setDisplayRows(rows);
      return;
    }
    if (!isPaused) {
      setDisplayRows(rows);
    }
  }, [isPaused, ladderWithPriceStep?.symbol, rows]);

  useLayoutEffect(() => {
    const signature = [
      ladderWithPriceStep?.symbol ?? "-",
      ladderWithPriceStep?.bucket_size ?? 0,
      effectiveRangeMultiplier,
      priceStepPreset,
      customPriceStep,
      displayRows.length,
    ].join("|");
    if (signature === lastCenterSignatureRef.current) return;
    lastCenterSignatureRef.current = signature;
    recenterLadder();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ladderWithPriceStep?.symbol, ladderWithPriceStep?.bucket_size, effectiveRangeMultiplier, priceStepPreset, customPriceStep, displayRows.length]);

  useEffect(() => {
    const container = listRef.current;
    if (!container) return;

    const detachForUser = () => {
      userDetachedUntilRef.current = Date.now() + 5000;
    };

    const onWheel = () => detachForUser();
    const onPointerDown = () => detachForUser();
    const onTouchStart = () => detachForUser();

    container.addEventListener("wheel", onWheel, { passive: true });
    container.addEventListener("pointerdown", onPointerDown, { passive: true });
    container.addEventListener("touchstart", onTouchStart, { passive: true });

    return () => {
      container.removeEventListener("wheel", onWheel);
      container.removeEventListener("pointerdown", onPointerDown);
      container.removeEventListener("touchstart", onTouchStart);
    };
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      smoothFollowCenter();
    }, 120);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [displayRows, ladderWithPriceStep, isPaused]);

  useEffect(() => {
    if (!showSettings) return;
    const onPointerDown = (event: PointerEvent) => {
      const node = panelRef.current;
      if (!node) return;
      if (!node.contains(event.target as Node)) {
        setShowSettings(false);
      }
    };
    window.addEventListener("pointerdown", onPointerDown);
    return () => window.removeEventListener("pointerdown", onPointerDown);
  }, [showSettings]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Element | null;
      if (!target) return;
      if (target.closest(".ladder-row")) return;
      setSelectedRowPrice(null);
    };
    window.addEventListener("pointerdown", onPointerDown);
    return () => window.removeEventListener("pointerdown", onPointerDown);
  }, []);

  const highlightedRowSet = useMemo(() => {
    const keys = new Set<number>();
    if (!markHighAmountArea || highAmountRows <= 0 || displayRows.length === 0) return keys;

    const topAsks = displayRows
      .filter((row) => row.side === "above")
      .sort((a, b) => b.liquidity - a.liquidity)
      .slice(0, highAmountRows);
    const topBids = displayRows
      .filter((row) => row.side === "below")
      .sort((a, b) => b.liquidity - a.liquidity)
      .slice(0, highAmountRows);

    for (const row of topAsks) keys.add(row.row);
    for (const row of topBids) keys.add(row.row);
    return keys;
  }, [displayRows, highAmountRows, markHighAmountArea]);

  const sizeThresholdRowSet = useMemo(() => {
    const keys = new Set<number>();
    if (!markSizeThreshold || displayRows.length === 0) return keys;
    const thresholdMinBase = Math.max(0, sizeThresholdMinAmount);
    const thresholdMaxBase = Math.max(0, sizeThresholdMaxAmount);
    const minEnabled = thresholdMinBase > 0;
    const maxEnabled = thresholdMaxBase > 0;
    if (!minEnabled && !maxEnabled) return keys;

    const multiplier = sizeThresholdUnit === "M" ? 1_000_000 : 1_000;
    let minValue = minEnabled ? thresholdMinBase * multiplier : 0;
    let maxValue = maxEnabled ? thresholdMaxBase * multiplier : Number.POSITIVE_INFINITY;
    if (minEnabled && maxEnabled && minValue > maxValue) {
      const temp = minValue;
      minValue = maxValue;
      maxValue = temp;
    }
    for (const row of displayRows) {
      const passMin = row.liquidity >= minValue;
      const passMax = row.liquidity <= maxValue;
      if (passMin && passMax) {
        keys.add(row.row);
      }
    }
    return keys;
  }, [displayRows, markSizeThreshold, sizeThresholdMaxAmount, sizeThresholdMinAmount, sizeThresholdUnit]);

  const chartOverlayLines = useMemo<ChartHorizontalLine[]>(() => {
    const lines: ChartHorizontalLine[] = [];
    if (showHighAmountOnChart) {
      for (const row of displayRows) {
        if (highlightedRowSet.has(row.row)) {
          lines.push({ price: row.price, color: "#f3d54a" });
        }
      }
    }
    if (showSizeThresholdOnChart) {
      for (const row of displayRows) {
        if (sizeThresholdRowSet.has(row.row)) {
          lines.push({ price: row.price, color: "#4aa2ff" });
        }
      }
    }
    if (showSelectedRowOnChart && selectedRowPrice !== null) {
      lines.push({ price: selectedRowPrice, color: "#111111" });
    }
    return lines;
  }, [
    displayRows,
    highlightedRowSet,
    selectedRowPrice,
    showHighAmountOnChart,
    showSelectedRowOnChart,
    showSizeThresholdOnChart,
    sizeThresholdRowSet,
  ]);

  useEffect(() => {
    onOverlayLinesChange?.(chartOverlayLines);
  }, [chartOverlayLines, onOverlayLinesChange]);

  useEffect(() => {
    return () => onOverlayLinesChange?.([]);
  }, [onOverlayLinesChange]);

  return (
    <aside className="price-ladder-panel" ref={panelRef}>
      <div className="price-ladder-header">
        <div className="small-title">Live Ladder</div>
        <div className="price-ladder-actions">
          <button className="ladder-pause-btn" onClick={() => setIsPaused((prev) => !prev)}>
            {isPaused ? "Play" : "Pause"}
          </button>
          <button className="ladder-settings-btn" onClick={() => setShowSettings((prev) => !prev)}>
            Cfg
          </button>
        </div>
      </div>
      {showSettings ? (
        <div className="ladder-settings-popover">
          <label className="ladder-settings-row">
            <input
              type="checkbox"
              checked={markHighAmountArea}
              onChange={(event) => setMarkHighAmountArea(event.target.checked)}
            />
            <span>Mark High Amount Area</span>
          </label>
          <label className="ladder-settings-row">
            <span>No</span>
            <input
              type="number"
              min={1}
              max={20}
              step={1}
              value={highAmountRows}
              onChange={(event) => {
                const value = Number(event.target.value);
                if (!Number.isFinite(value)) return;
                setHighAmountRows(Math.max(1, Math.min(20, Math.floor(value))));
              }}
            />
          </label>
          <label className="ladder-settings-row">
            <input
              type="checkbox"
              checked={showHighAmountOnChart}
              onChange={(event) => setShowHighAmountOnChart(event.target.checked)}
            />
            <span>Show Yellow On Chart</span>
          </label>
          <label className="ladder-settings-row">
            <input
              type="checkbox"
              checked={markSizeThreshold}
              onChange={(event) => setMarkSizeThreshold(event.target.checked)}
            />
            <span>Highlight Size Range</span>
          </label>
          <label className="ladder-settings-row">
            <span>Min</span>
            <input
              type="number"
              min={0}
              step={0.1}
              value={sizeThresholdMinAmount}
              disabled={!markSizeThreshold}
              onChange={(event) => {
                const value = Number(event.target.value);
                if (!Number.isFinite(value)) return;
                setSizeThresholdMinAmount(Math.max(0, value));
              }}
            />
          </label>
          <label className="ladder-settings-row">
            <span>Max</span>
            <input
              type="number"
              min={0}
              step={0.1}
              value={sizeThresholdMaxAmount}
              disabled={!markSizeThreshold}
              onChange={(event) => {
                const value = Number(event.target.value);
                if (!Number.isFinite(value)) return;
                setSizeThresholdMaxAmount(Math.max(0, value));
              }}
            />
          </label>
          <label className="ladder-settings-row">
            <span>Unit</span>
            <select
              value={sizeThresholdUnit}
              disabled={!markSizeThreshold}
              onChange={(event) => {
                const value = event.target.value === "M" ? "M" : "K";
                setSizeThresholdUnit(value);
              }}
            >
              <option value="K">K</option>
              <option value="M">M</option>
            </select>
          </label>
          <label className="ladder-settings-row">
            <input
              type="checkbox"
              checked={showSizeThresholdOnChart}
              onChange={(event) => setShowSizeThresholdOnChart(event.target.checked)}
            />
            <span>Show Blue On Chart</span>
          </label>
          <label className="ladder-settings-row">
            <input
              type="checkbox"
              checked={showSelectedRowOnChart}
              onChange={(event) => setShowSelectedRowOnChart(event.target.checked)}
            />
            <span>Show Selected On Chart</span>
          </label>
          <label className="ladder-settings-row">
            <span>Price Step</span>
            <select
              value={priceStepPreset}
              onChange={(event) => {
                const value = event.target.value;
                if (value === "auto" || value === "1" || value === "0.1" || value === "0.01" || value === "0.001" || value === "0.0001" || value === "custom") {
                  setPriceStepPreset(value);
                }
              }}
            >
              <option value="auto">Auto</option>
              <option value="1">1</option>
              <option value="0.1">0.1</option>
              <option value="0.01">0.01</option>
              <option value="0.001">0.001</option>
              <option value="0.0001">0.0001</option>
              <option value="custom">Custom</option>
            </select>
          </label>
          <label className="ladder-settings-row">
            <span>Ladder Range</span>
            <input
              type="range"
              min={1}
              max={10}
              step={1}
              value={ladderRangeMultiplier}
              onChange={(event) => {
                const value = Number(event.target.value);
                if (!Number.isFinite(value)) return;
                setLadderRangeMultiplier(Math.max(1, Math.min(10, Math.round(value))));
              }}
            />
          </label>
          <label className="ladder-settings-row">
            <span>Range Value</span>
            <strong>{ladderRangeMultiplier.toFixed(0)}x</strong>
          </label>
          {priceStepPreset === "custom" ? (
            <label className="ladder-settings-row">
              <span>Custom Step</span>
              <input
                type="text"
                inputMode="decimal"
                className="ladder-custom-step-input"
                value={customPriceStepInput}
                onChange={(event) => {
                  const next = event.target.value;
                  setCustomPriceStepInput(next);
                  if (next.trim() === "") return;
                  const value = Number(next);
                  if (!Number.isFinite(value) || value <= 0) return;
                  setCustomPriceStep(Math.max(0.00000001, value));
                }}
                onBlur={() => {
                  if (customPriceStepInput.trim() === "") {
                    setCustomPriceStepInput(formatStepInput(customPriceStep));
                    return;
                  }
                  const value = Number(customPriceStepInput);
                  if (!Number.isFinite(value) || value <= 0) {
                    setCustomPriceStepInput(formatStepInput(customPriceStep));
                    return;
                  }
                  const normalized = Math.max(0.00000001, value);
                  setCustomPriceStep(normalized);
                  setCustomPriceStepInput(formatStepInput(normalized));
                }}
              />
            </label>
          ) : null}
        </div>
      ) : null}
      <div
        className="price-ladder-rows"
        ref={listRef}
      >
        <div className="ladder-midline" />
        {displayRows.map((row) => (
          (() => {
            const isSelected = selectedRowPrice !== null && Math.abs(row.price - selectedRowPrice) <= selectedTolerance;
            return (
              <div
                key={row.row}
                className={`ladder-row ${row.side === "at" ? "current" : ""} ${row.side === "above" ? "ask" : ""} ${row.side === "below" ? "bid" : ""} ${highlightedRowSet.has(row.row) ? "high-amount" : ""} ${sizeThresholdRowSet.has(row.row) ? "size-threshold" : ""} ${isSelected ? "selected-row" : ""} ${isSelected && row.side === "below" ? "selected-buy-side" : ""} ${isSelected && row.side === "above" ? "selected-sell-side" : ""}`}
                onClick={() => setSelectedRowPrice(row.price)}
              >
                <div className="ladder-price">{row.price.toFixed(priceDecimals)}</div>
                <div className="ladder-box" style={{ background: ladderColor(row.side, row.intensity) }}>
                  {row.formatted}
                </div>
              </div>
            );
          })()
        ))}
      </div>
    </aside>
  );
}
