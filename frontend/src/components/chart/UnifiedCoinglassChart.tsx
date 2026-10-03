import { useEffect, useMemo, useRef, useState } from "react";
import { ChartHorizontalLine, ChartInteractionState, ChartViewportState, HeatmapColumn, KlinePoint, TerminalSnapshot } from "../../types/market";

type HeatmapStateView = {
  symbol: string;
  bucket_size: number;
  ticks_per_bucket: number;
  time_bucket_ms: number;
  max_columns: number;
  columns: HeatmapColumn[];
};

type UnifiedCoinglassChartProps = {
  klineSeries: KlinePoint[];
  snapshot: TerminalSnapshot | null;
  heatmap: HeatmapStateView | null;
  overlayLines?: ChartHorizontalLine[];
  onLiquidityProbe?: (probe: ChartLiquidityProbe | null) => void;
  onReplayCandleFocus?: (payload: { ts_ms: number; locked: boolean } | null) => void;
  drawingPersistenceKey?: string;
};

export type ChartLiquidityProbe = {
  center_bar_index: number;
  center_row_index: number;
  center_price: number;
  bucket_size: number;
  values: number[][];
};

type Candle = {
  ts_ms: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

type IndexedCandle = Candle & {
  bar_index: number;
  bar_span: number;
};

type IndexedHeatmapColumn = HeatmapColumn & {
  bar_index: number;
};

type DrawMode = "none" | "hline" | "rect" | "ruler";
type RulerSnapMode = "off" | "close" | "highlow";
type ReplayOverlayMode = "both" | "trace" | "aggregated";

type HLineDrawing = {
  id: string;
  kind: "hline";
  price: number;
  color: string;
};

type RectDrawing = {
  id: string;
  kind: "rect";
  bar_start: number;
  bar_end: number;
  price_low: number;
  price_high: number;
  color: string;
};

type RulerDrawing = {
  id: string;
  kind: "ruler";
  bar_start: number;
  bar_end: number;
  price_start: number;
  price_end: number;
  color: string;
};

type ChartDrawing = HLineDrawing | RectDrawing | RulerDrawing;

type DrawingHit =
  | { drawing_id: string; action: "hline_drag" }
  | { drawing_id: string; action: "rect_move" | "rect_left" | "rect_right" | "rect_top" | "rect_bottom" }
  | { drawing_id: string; action: "ruler_move" };

type DrawingDragState =
  | {
      kind: "hline_drag";
      drawing_id: string;
    }
  | {
      kind: "rect_create";
      anchor_bar: number;
      anchor_price: number;
    }
  | {
      kind: "rect_move" | "rect_left" | "rect_right" | "rect_top" | "rect_bottom";
      drawing_id: string;
      start_bar: number;
      start_price: number;
      origin: RectDrawing;
    }
  | {
      kind: "ruler_create";
      anchor_bar: number;
      anchor_price: number;
    }
  | {
      kind: "ruler_move";
      drawing_id: string;
      start_bar: number;
      start_price: number;
      origin: RulerDrawing;
    };

const TIMEFRAME_OPTIONS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"];
const DEFAULT_X_PIXELS_PER_BAR = 6;
const MIN_X_PIXELS_PER_BAR = 2;
const MAX_X_PIXELS_PER_BAR = 48;
const MIN_ROW_MULTIPLIER = 1;
const MAX_ROW_MULTIPLIER = 50;
const MIN_HEATMAP_COVERAGE_SCALE = 1;
const MAX_HEATMAP_COVERAGE_SCALE = 500;
const BASE_HEATMAP_RANGE_ROWS = 36;
const DATA_RATIO = 0.72;
const GAP_RATIO = 0.2;

function timeframeToMs(timeframe: string): number {
  if (timeframe.endsWith("m")) {
    const mins = Number(timeframe.slice(0, -1));
    return Number.isFinite(mins) && mins > 0 ? mins * 60_000 : 60_000;
  }
  if (timeframe.endsWith("h")) {
    const hours = Number(timeframe.slice(0, -1));
    return Number.isFinite(hours) && hours > 0 ? hours * 3_600_000 : 60_000;
  }
  return 60_000;
}

function aggregateCandles(series: KlinePoint[], timeframeMs: number): Candle[] {
  if (!series.length) return [];
  const buckets = new Map<number, Candle>();
  const ordered = [...series].sort((a, b) => a.open_ts_ms - b.open_ts_ms);
  for (const kline of ordered) {
    if (
      !Number.isFinite(kline.open) ||
      !Number.isFinite(kline.high) ||
      !Number.isFinite(kline.low) ||
      !Number.isFinite(kline.close) ||
      kline.open <= 0 ||
      kline.high <= 0 ||
      kline.low <= 0 ||
      kline.close <= 0
    ) {
      continue;
    }
    const bucket = Math.floor(kline.open_ts_ms / timeframeMs) * timeframeMs;
    const existing = buckets.get(bucket);
    if (!existing) {
      buckets.set(bucket, {
        ts_ms: bucket,
        open: kline.open,
        high: kline.high,
        low: kline.low,
        close: kline.close,
      });
      continue;
    }
    existing.high = Math.max(existing.high, kline.high);
    existing.low = Math.min(existing.low, kline.low);
    existing.close = kline.close;
  }
  return [...buckets.values()].sort((a, b) => a.ts_ms - b.ts_ms);
}

function aggregateHeatmapColumns(columns: HeatmapColumn[], sourceBucketMs: number, targetBucketMs: number): HeatmapColumn[] {
  if (!columns.length) return [];
  if (targetBucketMs <= sourceBucketMs) return columns;

  type Group = {
    ts_ms: number;
    center_row: number;
    row_min: number;
    row_max: number;
    rows: Map<number, number>;
  };

  const groups = new Map<number, Group>();
  for (const column of columns) {
    const bucketTs = Math.floor(column.ts_ms / targetBucketMs) * targetBucketMs;
    const existing = groups.get(bucketTs);
    if (!existing) {
      const nextRows = new Map<number, number>();
      for (const row of column.rows) {
        nextRows.set(row.row, row.value);
      }
      groups.set(bucketTs, {
        ts_ms: bucketTs,
        center_row: column.center_row,
        row_min: column.row_min,
        row_max: column.row_max,
        rows: nextRows,
      });
      continue;
    }

    existing.center_row = column.center_row;
    existing.row_min = Math.min(existing.row_min, column.row_min);
    existing.row_max = Math.max(existing.row_max, column.row_max);
    for (const row of column.rows) {
      const prev = existing.rows.get(row.row) ?? 0;
      existing.rows.set(row.row, Math.max(prev, row.value));
    }
  }

  return [...groups.values()]
    .sort((a, b) => a.ts_ms - b.ts_ms)
    .map((group) => ({
      ts_ms: group.ts_ms,
      center_row: group.center_row,
      row_min: group.row_min,
      row_max: group.row_max,
      rows: [...group.rows.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([row, value]) => ({ row, value })),
    }));
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function normalizeRect(anchorBar: number, anchorPrice: number, currentBar: number, currentPrice: number): RectDrawing {
  return {
    id: "",
    kind: "rect",
    bar_start: Math.min(anchorBar, currentBar),
    bar_end: Math.max(anchorBar, currentBar),
    price_low: Math.min(anchorPrice, currentPrice),
    price_high: Math.max(anchorPrice, currentPrice),
    color: "#2e89ff",
  };
}

function formatSigned(value: number, digits: number): string {
  if (!Number.isFinite(value)) return "-";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
}

function formatDurationCompact(ms: number): string {
  const clamped = Math.max(0, Math.round(ms));
  const totalSec = Math.floor(clamped / 1000);
  const mins = Math.floor(totalSec / 60);
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  if (hours > 0) return `${hours}h ${remMins}m`;
  return `${mins} min`;
}

function coverageToBaseRowRangeLimit(scale: number): number {
  const s = Math.max(1, scale);
  // Keep 1x near current behavior, but avoid saturating too early.
  // This makes 50x..200x continue to expand visible price coverage.
  return Math.max(1, Math.round(BASE_HEATMAP_RANGE_ROWS + (s - 1) * 3));
}

function coverageToGroupedRangeLimit(scale: number, rowBinMultiplier: number): number {
  const baseRows = coverageToBaseRowRangeLimit(scale);
  const groupedRows = baseRows / Math.max(1, rowBinMultiplier);
  return Math.max(1, Math.round(groupedRows));
}

function percentile(sortedValues: number[], p: number): number {
  if (!sortedValues.length) return 0;
  const clampedP = clamp(p, 0, 1);
  const index = Math.floor((sortedValues.length - 1) * clampedP);
  return sortedValues[index];
}

function niceStep(rawStep: number): number {
  if (!Number.isFinite(rawStep) || rawStep <= 0) return 1;
  const exponent = Math.floor(Math.log10(rawStep));
  const base = 10 ** exponent;
  const normalized = rawStep / base;
  if (normalized <= 1) return base;
  if (normalized <= 2) return 2 * base;
  if (normalized <= 5) return 5 * base;
  return 10 * base;
}

function formatPrice(price: number): string {
  if (!Number.isFinite(price)) return "-";
  if (Math.abs(price) >= 1_000) return price.toFixed(2);
  if (Math.abs(price) >= 1) return price.toFixed(4);
  return price.toFixed(6);
}

function formatLiquidity(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0.0";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(1);
}

function heatmapColor(intensity: number): string {
  const stops = [
    [245, 238, 210],
    [237, 186, 168],
    [206, 98, 118],
    [120, 44, 119],
  ];
  const clamped = clamp(intensity, 0, 1);
  const scaled = clamped * (stops.length - 1);
  const index = Math.floor(scaled);
  const nextIndex = Math.min(stops.length - 1, index + 1);
  const t = scaled - index;
  const a = stops[index];
  const b = stops[nextIndex];
  const red = Math.round(a[0] + (b[0] - a[0]) * t);
  const green = Math.round(a[1] + (b[1] - a[1]) * t);
  const blue = Math.round(a[2] + (b[2] - a[2]) * t);
  return `rgb(${red}, ${green}, ${blue})`;
}

export default function UnifiedCoinglassChart({
  klineSeries,
  snapshot,
  heatmap,
  overlayLines = [],
  onLiquidityProbe,
  onReplayCandleFocus,
  drawingPersistenceKey,
}: UnifiedCoinglassChartProps) {
  const panelRef = useRef<HTMLElement | null>(null);
  const chartWrapRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const axisStepRef = useRef<number>(0);
  const dragRef = useRef<{
    active: boolean;
    target: "plot" | "price_axis" | null;
    last_x: number;
    last_y: number;
  }>({
    active: false,
    target: null,
    last_x: 0,
    last_y: 0,
  });
  const rafPointerRef = useRef<number | null>(null);
  const pendingPointerRef = useRef<{ x: number; y: number } | null>(null);
  const yRangeRef = useRef<{ min: number; max: number }>({ min: 0, max: 1 });
  const drawingIdRef = useRef(0);
  const drawingDragRef = useRef<DrawingDragState | null>(null);
  const lastReplayBubblesRef = useRef<
    Record<
      string,
      {
        short_count?: number;
        long_count?: number;
        hold_count?: number;
        avg_predicted_magnitude_pct?: number;
      }
    >
  >({});

  const [size, setSize] = useState({ width: 900, height: 520 });
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [timeframe, setTimeframe] = useState("1m");
  const [showHeatmap, setShowHeatmap] = useState(true);
  const [thresholdLow, setThresholdLow] = useState(0.06);
  const [thresholdHigh, setThresholdHigh] = useState(0.88);
  const [rowSizeMultiplier, setRowSizeMultiplier] = useState(1);
  const [heatmapCoverageScale, setHeatmapCoverageScale] = useState(1);
  const [viewport, setViewport] = useState<ChartViewportState>({
    x_offset_bars: 0,
    x_pixels_per_bar: DEFAULT_X_PIXELS_PER_BAR,
    right_padding_bars: 0,
    follow_live: true,
    y_min: 0,
    y_max: 0,
    y_mode: "auto",
  });
  const [interaction, setInteraction] = useState<ChartInteractionState>({
    crosshair_x: null,
    crosshair_y: null,
    active_bar_index: null,
    active_price: null,
    is_dragging: false,
    drag_target: null,
  });
  const [fullscreenProbe, setFullscreenProbe] = useState<ChartLiquidityProbe | null>(null);
  const [lockedReplayCandleTs, setLockedReplayCandleTs] = useState<number | null>(null);
  const [hoverReplayCandleTs, setHoverReplayCandleTs] = useState<number | null>(null);
  const [drawMode, setDrawMode] = useState<DrawMode>("none");
  const [rulerSnapMode, setRulerSnapMode] = useState<RulerSnapMode>("off");
  const [replayOverlayMode, setReplayOverlayMode] = useState<ReplayOverlayMode>("both");
  const [drawings, setDrawings] = useState<ChartDrawing[]>([]);
  const [activeDrawingId, setActiveDrawingId] = useState<string | null>(null);
  const [rectDraft, setRectDraft] = useState<RectDrawing | null>(null);
  const [rulerDraft, setRulerDraft] = useState<RulerDrawing | null>(null);

  const timeframeMs = useMemo(() => timeframeToMs(timeframe), [timeframe]);
  const candles = useMemo(() => aggregateCandles(klineSeries, timeframeMs), [klineSeries, timeframeMs]);
  const rawHeatmapColumns = heatmap?.columns ?? [];
  const bucketSize = heatmap?.bucket_size ?? 0;
  const sourceBucketMs = heatmap?.time_bucket_ms ?? timeframeMs;
  const displayBucketMs = Math.max(sourceBucketMs, timeframeMs);
  const replayModeLayer = (snapshot as unknown as { decision_layer?: Record<string, unknown> } | null)?.decision_layer ?? {};
  const replayModeRaw = String(replayModeLayer["replay_data_mode"] ?? "").toLowerCase();
  const replayModeBySnapshot =
    replayModeRaw.includes("replay") ||
    replayModeRaw.includes("prediction_trace") ||
    replayModeRaw.includes("trace") ||
    (Array.isArray(replayModeLayer["replay_history"]) && (replayModeLayer["replay_history"] as unknown[]).length > 0) ||
    Boolean(replayModeLayer["replay_candle_bubbles"] && typeof replayModeLayer["replay_candle_bubbles"] === "object") ||
    Boolean(replayModeLayer["replay_candle_core_by_ts"] && typeof replayModeLayer["replay_candle_core_by_ts"] === "object");
  const heatmapColumns = useMemo(
    () => aggregateHeatmapColumns(rawHeatmapColumns, sourceBucketMs, displayBucketMs),
    [rawHeatmapColumns, sourceBucketMs, displayBucketMs]
  );
  const hasHeatmapData = !replayModeBySnapshot && showHeatmap && bucketSize > 0 && heatmapColumns.length > 0;
  const originTs = useMemo(() => {
    const candidates: number[] = [];
    if (candles.length) candidates.push(candles[0].ts_ms);
    if (heatmapColumns.length) {
      candidates.push(Math.floor(heatmapColumns[0].ts_ms / timeframeMs) * timeframeMs);
    }
    return candidates.length ? Math.min(...candidates) : 0;
  }, [candles, heatmapColumns, timeframeMs]);

  const indexedHeatmapColumns = useMemo<IndexedHeatmapColumn[]>(() => {
    if (!hasHeatmapData || originTs <= 0) return [];
    return heatmapColumns
      .map((column) => {
        const alignedTs = Math.floor(column.ts_ms / timeframeMs) * timeframeMs;
        return {
          ...column,
          bar_index: Math.floor((alignedTs - originTs) / timeframeMs),
        };
      })
      .filter((column) => column.bar_index >= 0)
      .sort((a, b) => a.bar_index - b.bar_index);
  }, [hasHeatmapData, heatmapColumns, originTs, timeframeMs]);

  const heatmapByBarIndex = useMemo(() => {
    const table = new Map<number, IndexedHeatmapColumn>();
    for (const column of indexedHeatmapColumns) {
      const existing = table.get(column.bar_index);
      if (!existing || column.ts_ms > existing.ts_ms) {
        table.set(column.bar_index, column);
      }
    }
    return table;
  }, [indexedHeatmapColumns]);

  const layout = useMemo(() => {
    const axisWidth = Math.max(70, Math.floor(size.width * (1 - DATA_RATIO - GAP_RATIO)));
    const dataWidth = Math.max(180, Math.floor(size.width * DATA_RATIO));
    const gapWidth = Math.max(80, size.width - dataWidth - axisWidth);
    return {
      dataWidth,
      gapWidth,
      axisWidth,
      plotWidth: dataWidth + gapWidth,
      plotHeight: size.height,
    };
  }, [size.height, size.width]);

  const totalBars = useMemo(() => {
    const lastCandleBar =
      candles.length > 0 && originTs > 0
        ? Math.floor((candles[candles.length - 1].ts_ms - originTs) / timeframeMs) + 1
        : 0;
    const lastHeatmapBar = indexedHeatmapColumns.length > 0 ? indexedHeatmapColumns[indexedHeatmapColumns.length - 1].bar_index + 1 : 0;
    return Math.max(1, lastCandleBar, lastHeatmapBar);
  }, [candles, originTs, timeframeMs, indexedHeatmapColumns]);

  useEffect(() => {
    const nextPaddingBars = layout.gapWidth / Math.max(1, viewport.x_pixels_per_bar);
    setViewport((prev) => {
      if (Math.abs(prev.right_padding_bars - nextPaddingBars) < 0.001) return prev;
      return { ...prev, right_padding_bars: nextPaddingBars };
    });
  }, [layout.gapWidth, viewport.x_pixels_per_bar]);

  const visibleBars = useMemo(
    () => layout.plotWidth / Math.max(MIN_X_PIXELS_PER_BAR, viewport.x_pixels_per_bar),
    [layout.plotWidth, viewport.x_pixels_per_bar]
  );

  const liveFirstBar = totalBars - (visibleBars - viewport.right_padding_bars);
  const firstVisibleBar = viewport.follow_live ? liveFirstBar : liveFirstBar - viewport.x_offset_bars;
  const lastVisibleBar = firstVisibleBar + visibleBars;

  const indexedCandles = useMemo<IndexedCandle[]>(() => {
    if (!candles.length || originTs <= 0) return [];
    return candles.map((candle) => ({
      ...candle,
      bar_index: Math.floor((candle.ts_ms - originTs) / timeframeMs),
      bar_span: 1,
    }));
  }, [candles, originTs, timeframeMs]);
  const isReplayChart = replayModeBySnapshot;
  const replayCursor = useMemo(() => {
    const layer = (snapshot as unknown as { decision_layer?: Record<string, unknown> } | null)?.decision_layer ?? {};
    const v = Number(layer["replay_cursor"] ?? NaN);
    return Number.isFinite(v) ? v : null;
  }, [snapshot]);
  const replayFocusedCandleTs = lockedReplayCandleTs ?? hoverReplayCandleTs;
  function nearestCandleTsForBar(activeBar: number | null): number | null {
    if (activeBar === null || indexedCandles.length === 0) return null;
    let nearestTs: number | null = null;
    let bestDist = Number.POSITIVE_INFINITY;
    for (const candle of indexedCandles) {
      const d = Math.abs(candle.bar_index - activeBar);
      if (d < bestDist) {
        bestDist = d;
        nearestTs = candle.ts_ms;
      }
    }
    return nearestTs;
  }
  const replayFullscreenRows = useMemo(() => {
    const layer = (snapshot as unknown as { decision_layer?: Record<string, unknown> } | null)?.decision_layer ?? {};
    const fallbackDecision =
      layer["replay_decision"] && typeof layer["replay_decision"] === "object"
        ? (layer["replay_decision"] as Record<string, unknown>)
        : null;
    const history = Array.isArray(layer["replay_history"]) ? (layer["replay_history"] as unknown[]) : [];
    const coreByTs =
      layer["replay_candle_core_by_ts"] && typeof layer["replay_candle_core_by_ts"] === "object"
        ? (layer["replay_candle_core_by_ts"] as Record<string, unknown>)
        : null;
    let decision: Record<string, unknown> | null = null;
    if (Number.isFinite(Number(replayFocusedCandleTs ?? NaN)) && (history.length > 0 || coreByTs)) {
      const target = Number(replayFocusedCandleTs);
      const bucketTarget = Math.floor(target / 60_000) * 60_000;
      const exact = coreByTs?.[String(bucketTarget)];
      if (exact && typeof exact === "object") {
        decision = exact as Record<string, unknown>;
      }
      let bestDist = Number.POSITIVE_INFINITY;
      for (const item of history) {
        if (!item || typeof item !== "object") continue;
        const row = item as Record<string, unknown>;
        const ts = Number(row.plot_ts_ms ?? row.ts_ms ?? NaN);
        if (!Number.isFinite(ts)) continue;
        if (!decision && Math.floor(ts / 60_000) === Math.floor(target / 60_000)) {
          decision = row;
          continue;
        }
        const d = Math.abs(ts - target);
        if (d < bestDist) {
          bestDist = d;
          decision = row;
        }
      }
    }
    if (!decision) decision = fallbackDecision;
    const fmtNum = (value: unknown, digits = 4) => {
      const n = Number(value);
      return Number.isFinite(n) ? n.toFixed(digits) : "-";
    };
    if (!decision) {
      return Array.from({ length: 8 }).map((_, idx) => ({ label: `Row ${idx + 1}`, value: "-" }));
    }
    return [
      { label: "Action", value: String(decision.action ?? decision.predicted_label ?? "HOLD").toUpperCase() },
      {
        label: "Prob D/F/U",
        value: `${fmtNum(decision.prob_down, 3)} / ${fmtNum(decision.prob_flat, 3)} / ${fmtNum(decision.prob_up, 3)}`,
      },
      {
        label: "Conf/Gap/Qual",
        value: `${fmtNum(decision.confidence, 3)} / ${fmtNum(decision.confidence_gap, 3)} / ${fmtNum(decision.quality, 3)}`,
      },
      { label: "Pred Move", value: `${fmtNum(decision.predicted_magnitude_pct ?? decision.magnitude_pct, 4)}%` },
      { label: "Actual Move", value: `${fmtNum(decision.actual_future_return_pct, 4)}%` },
      { label: "Correct", value: decision.correct ? "yes" : "no" },
      {
        label: "Gate",
        value: decision.entry_allowed ? "entry_allowed" : `blocked (${String(decision.blocked_reason || "other_gate")})`,
      },
      { label: "Price @ Pred", value: fmtNum(decision.price_at_prediction, 6) },
    ];
  }, [snapshot, replayFocusedCandleTs]);

  useEffect(() => {
    if (!drawingPersistenceKey) return;
    try {
      const raw = window.localStorage.getItem(`chart_rulers_v1:${drawingPersistenceKey}`);
      if (!raw) return;
      const parsed = JSON.parse(raw) as unknown;
      if (!Array.isArray(parsed)) return;
      const restored = parsed
        .filter((item) => !!item && typeof item === "object")
        .map((item) => item as Partial<RulerDrawing>)
        .filter((item) => item.kind === "ruler" && typeof item.id === "string")
        .map((item) => ({
          id: String(item.id),
          kind: "ruler" as const,
          bar_start: Number(item.bar_start ?? 0),
          bar_end: Number(item.bar_end ?? 0),
          price_start: Number(item.price_start ?? 0),
          price_end: Number(item.price_end ?? 0),
          color: String(item.color ?? "#2e89ff"),
        }))
        .filter(
          (item) =>
            Number.isFinite(item.bar_start) &&
            Number.isFinite(item.bar_end) &&
            Number.isFinite(item.price_start) &&
            Number.isFinite(item.price_end)
        );
      if (restored.length > 0) setDrawings((prev) => [...prev.filter((d) => d.kind !== "ruler"), ...restored]);
    } catch {
      // Ignore malformed local storage values.
    }
  }, [drawingPersistenceKey]);

  useEffect(() => {
    if (!drawingPersistenceKey) return;
    try {
      const rulers = drawings.filter((d): d is RulerDrawing => d.kind === "ruler");
      window.localStorage.setItem(`chart_rulers_v1:${drawingPersistenceKey}`, JSON.stringify(rulers));
    } catch {
      // Ignore storage write errors.
    }
  }, [drawings, drawingPersistenceKey]);

  const autoRange = useMemo(() => {
    let minPrice = Number.POSITIVE_INFINITY;
    let maxPrice = Number.NEGATIVE_INFINITY;
    const fromBar = Math.floor(firstVisibleBar) - 2;
    const toBar = Math.ceil(lastVisibleBar) + 2;

    for (const candle of indexedCandles) {
      const candleEnd = candle.bar_index + candle.bar_span;
      if (candleEnd < fromBar || candle.bar_index > toBar) continue;
      minPrice = Math.min(minPrice, candle.low);
      maxPrice = Math.max(maxPrice, candle.high);
    }

    if (hasHeatmapData && bucketSize > 0) {
      const rowBinMultiplier = Math.max(1, rowSizeMultiplier);
      const coverageScale = Math.max(1, heatmapCoverageScale);
      const anchorPrice =
        snapshot?.last_trade_price ||
        snapshot?.mark_price ||
        (Number.isFinite(minPrice) && Number.isFinite(maxPrice) ? (minPrice + maxPrice) / 2 : 1);
      const anchorRow = anchorPrice / bucketSize;
      const groupedRangeLimit = coverageToGroupedRangeLimit(coverageScale, rowBinMultiplier);
      const effectiveGroupedRangeLimit = isReplayChart ? Math.max(groupedRangeLimit, 2000) : groupedRangeLimit;
      const groupedSpanPrice = groupedRangeLimit * rowBinMultiplier * bucketSize;
      const baseRowSpanPrice = rowBinMultiplier * bucketSize;
      const theoreticalHeatMin = anchorPrice - groupedSpanPrice - baseRowSpanPrice;
      const theoreticalHeatMax = anchorPrice + groupedSpanPrice + baseRowSpanPrice;
      let heatMin = Number.POSITIVE_INFINITY;
      let heatMax = Number.NEGATIVE_INFINITY;

      for (let bar = fromBar; bar <= toBar; bar += 1) {
        const column = heatmapByBarIndex.get(bar);
        if (!column) continue;
        const columnCenterRowRaw = Number(column.center_row);
        const columnCenterRow = isReplayChart && Number.isFinite(columnCenterRowRaw) ? columnCenterRowRaw : anchorRow;
        const anchorGroupedRow = Math.floor(columnCenterRow / rowBinMultiplier);
        for (const cell of column.rows) {
          const groupedRow = Math.floor(cell.row / rowBinMultiplier);
          if (Math.abs(groupedRow - anchorGroupedRow) > effectiveGroupedRangeLimit) continue;
          const baseRow = groupedRow * rowBinMultiplier;
          const lower = anchorPrice + (baseRow - columnCenterRow) * bucketSize;
          const upper = anchorPrice + (baseRow + rowBinMultiplier - columnCenterRow) * bucketSize;
          heatMin = Math.min(heatMin, lower, upper);
          heatMax = Math.max(heatMax, lower, upper);
        }
      }

      // In replay mode, using theoretical expansion can explode Y-range and hide candles.
      // Prefer actual heatmap rows there; keep theoretical expansion only for live mode.
      if (isReplayChart) {
        if (Number.isFinite(heatMin) && Number.isFinite(heatMax) && heatMin < heatMax) {
          const heatRange = Math.max(0.0000001, heatMax - heatMin);
          const heatPad = heatRange * 0.08;
          const expandedHeatMin = heatMin - heatPad;
          const expandedHeatMax = heatMax + heatPad;
          minPrice = Number.isFinite(minPrice) ? Math.min(minPrice, expandedHeatMin) : expandedHeatMin;
          maxPrice = Number.isFinite(maxPrice) ? Math.max(maxPrice, expandedHeatMax) : expandedHeatMax;
        }
      } else if (
        Number.isFinite(theoreticalHeatMin) &&
        Number.isFinite(theoreticalHeatMax) &&
        theoreticalHeatMin < theoreticalHeatMax
      ) {
        const resolvedHeatMin = Number.isFinite(heatMin) ? Math.min(heatMin, theoreticalHeatMin) : theoreticalHeatMin;
        const resolvedHeatMax = Number.isFinite(heatMax) ? Math.max(heatMax, theoreticalHeatMax) : theoreticalHeatMax;
        const heatRange = Math.max(0.0000001, resolvedHeatMax - resolvedHeatMin);
        const heatPad = heatRange * 0.12;
        const expandedHeatMin = resolvedHeatMin - heatPad;
        const expandedHeatMax = resolvedHeatMax + heatPad;

        minPrice = Number.isFinite(minPrice) ? Math.min(minPrice, expandedHeatMin) : expandedHeatMin;
        maxPrice = Number.isFinite(maxPrice) ? Math.max(maxPrice, expandedHeatMax) : expandedHeatMax;
      }
    }

    if (!Number.isFinite(minPrice) || !Number.isFinite(maxPrice) || minPrice >= maxPrice) {
      const ref = snapshot?.last_trade_price || snapshot?.mark_price || 1;
      minPrice = ref * 0.99;
      maxPrice = ref * 1.01;
    }
    const range = Math.max(0.0000001, maxPrice - minPrice);
    const margin = range * 0.02;
    return {
      min: minPrice - margin,
      max: maxPrice + margin,
    };
  }, [
    firstVisibleBar,
    lastVisibleBar,
    indexedCandles,
    hasHeatmapData,
    heatmapByBarIndex,
    bucketSize,
    rowSizeMultiplier,
    heatmapCoverageScale,
    isReplayChart,
    snapshot?.last_trade_price,
    snapshot?.mark_price,
  ]);

  const yMin = viewport.y_mode === "auto" ? autoRange.min : viewport.y_min;
  const yMax = viewport.y_mode === "auto" ? autoRange.max : viewport.y_max;
  const yRange = Math.max(0.0000001, yMax - yMin);

  useEffect(() => {
    yRangeRef.current = { min: yMin, max: yMax };
  }, [yMax, yMin]);

  useEffect(() => {
    const element = chartWrapRef.current;
    if (!element) return;
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect) return;
      setSize({
        width: Math.max(320, Math.floor(rect.width)),
        height: Math.max(280, Math.floor(rect.height)),
      });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const onFullscreenChange = () => {
      const element = panelRef.current;
      setIsFullscreen(Boolean(element && document.fullscreenElement === element));
    };
    document.addEventListener("fullscreenchange", onFullscreenChange);
    return () => document.removeEventListener("fullscreenchange", onFullscreenChange);
  }, []);

  useEffect(() => {
    if (!isReplayChart) {
      setLockedReplayCandleTs(null);
      setHoverReplayCandleTs(null);
      onReplayCandleFocus?.(null);
      return;
    }
    if (Number.isFinite(Number(replayFocusedCandleTs ?? NaN))) {
      onReplayCandleFocus?.({ ts_ms: Number(replayFocusedCandleTs), locked: lockedReplayCandleTs !== null });
      return;
    }
    onReplayCandleFocus?.(null);
  }, [isReplayChart, replayFocusedCandleTs, lockedReplayCandleTs, onReplayCandleFocus]);

  useEffect(() => {
    if (!isReplayChart) return;
    // Keep replay window anchored when cursor changes so candles stay visible
    // throughout seek/scrub, even if previous pan state was detached.
    setViewport((prev) => {
      if (prev.follow_live && prev.x_offset_bars <= 0.001) return prev;
      return {
        ...prev,
        follow_live: true,
        x_offset_bars: 0,
      };
    });
  }, [isReplayChart, replayCursor, klineSeries.length]);

  function xToBar(x: number): number {
    return firstVisibleBar + x / Math.max(MIN_X_PIXELS_PER_BAR, viewport.x_pixels_per_bar);
  }

  function barToX(barIndex: number): number {
    return (barIndex - firstVisibleBar) * viewport.x_pixels_per_bar;
  }

  function priceToY(price: number): number {
    const ratio = (yMax - price) / yRange;
    return clamp(ratio, 0, 1) * layout.plotHeight;
  }

  function yToPrice(y: number): number {
    const ratio = clamp(y / layout.plotHeight, 0, 1);
    return yMax - ratio * yRange;
  }

  function pointerToBarPrice(x: number, y: number): { bar: number; price: number } {
    const clampedX = clamp(x, 0, layout.plotWidth);
    const clampedY = clamp(y, 0, layout.plotHeight);
    return {
      bar: xToBar(clampedX),
      price: yToPrice(clampedY),
    };
  }

  function snapPriceForBar(bar: number, rawPrice: number): number {
    if (rulerSnapMode === "off" || indexedCandles.length === 0) return rawPrice;
    let nearest: IndexedCandle | null = null;
    let bestDist = Number.POSITIVE_INFINITY;
    for (const candle of indexedCandles) {
      const d = Math.abs(candle.bar_index - bar);
      if (d < bestDist) {
        bestDist = d;
        nearest = candle;
      }
    }
    if (!nearest) return rawPrice;
    if (rulerSnapMode === "close") return nearest.close;
    const distHigh = Math.abs(rawPrice - nearest.high);
    const distLow = Math.abs(rawPrice - nearest.low);
    const distClose = Math.abs(rawPrice - nearest.close);
    if (distHigh <= distLow && distHigh <= distClose) return nearest.high;
    if (distLow <= distHigh && distLow <= distClose) return nearest.low;
    return nearest.close;
  }

  function nextDrawingId(): string {
    drawingIdRef.current += 1;
    return `draw_${drawingIdRef.current}`;
  }

  function findDrawingHit(x: number, y: number): DrawingHit | null {
    const pxTol = 7;
    for (let i = drawings.length - 1; i >= 0; i -= 1) {
      const drawing = drawings[i];
      if (drawing.kind === "hline") {
        const lineY = priceToY(drawing.price);
        if (Math.abs(y - lineY) <= pxTol) {
          return { drawing_id: drawing.id, action: "hline_drag" };
        }
        continue;
      }
      if (drawing.kind === "ruler") {
        const x1 = barToX(drawing.bar_start);
        const y1 = priceToY(drawing.price_start);
        const x2 = barToX(drawing.bar_end);
        const y2 = priceToY(drawing.price_end);
        const dx = x2 - x1;
        const dy = y2 - y1;
        const len2 = dx * dx + dy * dy;
        if (len2 < 1) continue;
        const t = clamp(((x - x1) * dx + (y - y1) * dy) / len2, 0, 1);
        const px = x1 + t * dx;
        const py = y1 + t * dy;
        const dist = Math.hypot(x - px, y - py);
        if (dist <= pxTol + 1) {
          return { drawing_id: drawing.id, action: "ruler_move" };
        }
        continue;
      }

      const left = Math.min(barToX(drawing.bar_start), barToX(drawing.bar_end));
      const right = Math.max(barToX(drawing.bar_start), barToX(drawing.bar_end));
      const top = Math.min(priceToY(drawing.price_high), priceToY(drawing.price_low));
      const bottom = Math.max(priceToY(drawing.price_high), priceToY(drawing.price_low));
      const inside = x >= left && x <= right && y >= top && y <= bottom;
      if (!inside) continue;

      const nearLeft = Math.abs(x - left) <= pxTol;
      const nearRight = Math.abs(x - right) <= pxTol;
      const nearTop = Math.abs(y - top) <= pxTol;
      const nearBottom = Math.abs(y - bottom) <= pxTol;

      if (nearLeft) return { drawing_id: drawing.id, action: "rect_left" };
      if (nearRight) return { drawing_id: drawing.id, action: "rect_right" };
      if (nearTop) return { drawing_id: drawing.id, action: "rect_top" };
      if (nearBottom) return { drawing_id: drawing.id, action: "rect_bottom" };
      return { drawing_id: drawing.id, action: "rect_move" };
    }
    return null;
  }

  function resetHorizontal() {
    setViewport((prev) => ({
      ...prev,
      follow_live: true,
      x_offset_bars: 0,
      x_pixels_per_bar: DEFAULT_X_PIXELS_PER_BAR,
    }));
  }

  function resetYAuto() {
    setViewport((prev) => ({
      ...prev,
      y_mode: "auto",
    }));
  }

  async function toggleFullscreen() {
    const element = panelRef.current;
    if (!element) return;
    try {
      if (document.fullscreenElement === element) {
        await document.exitFullscreen();
      } else {
        await element.requestFullscreen();
      }
    } catch {
      // Ignore fullscreen API errors (browser restrictions/user gesture issues).
    }
  }

  function updateCrosshair(x: number, y: number) {
    if (x < 0 || x > layout.plotWidth || y < 0 || y > layout.plotHeight) {
      setInteraction((prev) => ({
        ...prev,
        crosshair_x: null,
        crosshair_y: null,
        active_bar_index: null,
        active_price: null,
      }));
      setFullscreenProbe(null);
      onLiquidityProbe?.(null);
      return;
    }
    const activeBar = Math.round(xToBar(x));
    const activePrice = yToPrice(y);
    setInteraction((prev) => ({
      ...prev,
      crosshair_x: x,
      crosshair_y: y,
      active_bar_index: activeBar,
      active_price: activePrice,
    }));
    if (isReplayChart && lockedReplayCandleTs === null) {
      setHoverReplayCandleTs(nearestCandleTsForBar(activeBar));
    }

    if (!hasHeatmapData || bucketSize <= 0) {
      setFullscreenProbe(null);
      onLiquidityProbe?.(null);
      return;
    }
    const rowBinMultiplier = Math.max(1, rowSizeMultiplier);
    const anchorPrice = snapshot?.last_trade_price || snapshot?.mark_price || activePrice;
    const anchorRow = anchorPrice / bucketSize;
    const displayRowStep = bucketSize * rowBinMultiplier;
    const centerRow = Math.floor((activePrice - anchorPrice) / displayRowStep);
    const rowTargets = [centerRow + 1, centerRow, centerRow - 1];
    const colTargets = [activeBar - 1, activeBar, activeBar + 1];

    const values = rowTargets.map((targetRow) =>
      colTargets.map((targetCol) => {
        const column = heatmapByBarIndex.get(targetCol);
        if (!column) return 0;
        let sum = 0;
        for (const cell of column.rows) {
          const groupedRow = Math.floor(cell.row / rowBinMultiplier);
          const baseRow = groupedRow * rowBinMultiplier;
          const cellCenterPrice =
            anchorPrice + (baseRow + rowBinMultiplier * 0.5 - anchorRow) * bucketSize;
          const cellDisplayRow = Math.floor((cellCenterPrice - anchorPrice) / displayRowStep);
          if (cellDisplayRow === targetRow) {
            sum += cell.value;
          }
        }
        return sum;
      })
    );

    const probePayload: ChartLiquidityProbe = {
      center_bar_index: activeBar,
      center_row_index: centerRow,
      center_price: activePrice,
      bucket_size: displayRowStep,
      values,
    };
    setFullscreenProbe(probePayload);
    onLiquidityProbe?.(probePayload);
  }

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(size.width * dpr);
    canvas.height = Math.floor(size.height * dpr);
    canvas.style.width = `${size.width}px`;
    canvas.style.height = `${size.height}px`;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    ctx.clearRect(0, 0, size.width, size.height);
    ctx.fillStyle = "#f4efd9";
    ctx.fillRect(0, 0, layout.plotWidth, layout.plotHeight);
    ctx.fillStyle = "rgba(255, 255, 255, 0.26)";
    ctx.fillRect(layout.dataWidth, 0, layout.gapWidth, layout.plotHeight);
    ctx.fillStyle = "rgba(255, 255, 255, 0.18)";
    ctx.fillRect(layout.plotWidth, 0, layout.axisWidth, layout.plotHeight);

    ctx.strokeStyle = "rgba(0,0,0,0.08)";
    ctx.beginPath();
    ctx.moveTo(layout.dataWidth + 0.5, 0);
    ctx.lineTo(layout.dataWidth + 0.5, layout.plotHeight);
    ctx.moveTo(layout.plotWidth + 0.5, 0);
    ctx.lineTo(layout.plotWidth + 0.5, layout.plotHeight);
    ctx.stroke();

    if (hasHeatmapData && bucketSize > 0) {
      const prevAlpha = ctx.globalAlpha;
      ctx.globalAlpha = isReplayChart ? 0.45 : 1;
      let drawnHeatCells = 0;
      const fromBar = Math.floor(firstVisibleBar) - 2;
      const toBar = Math.ceil(lastVisibleBar) + 2;
      const rowBinMultiplier = Math.max(1, rowSizeMultiplier);
      const coverageScale = Math.max(1, heatmapCoverageScale);
      const anchorPrice = snapshot?.last_trade_price || snapshot?.mark_price || (yMin + yMax) / 2;
      const anchorRow = anchorPrice / bucketSize;
      const groupedRangeLimit = coverageToGroupedRangeLimit(coverageScale, rowBinMultiplier);
      const effectiveGroupedRangeLimit = isReplayChart ? Math.max(groupedRangeLimit, 2000) : groupedRangeLimit;
      let maxLog = 1;
      const transformedSamples: number[] = [];
      for (let index = fromBar; index <= toBar; index += 1) {
        const column = heatmapByBarIndex.get(index);
        if (!column) continue;
        const columnCenterRowRaw = Number(column.center_row);
        const columnCenterRow = isReplayChart && Number.isFinite(columnCenterRowRaw) ? columnCenterRowRaw : anchorRow;
        const anchorGroupedRow = Math.floor(columnCenterRow / rowBinMultiplier);
        const grouped = new Map<number, number>();
        for (const cell of column.rows) {
          const groupedRow = Math.floor(cell.row / rowBinMultiplier);
          if (Math.abs(groupedRow - anchorGroupedRow) > effectiveGroupedRangeLimit) continue;
          grouped.set(groupedRow, (grouped.get(groupedRow) ?? 0) + cell.value);
        }
        for (const value of grouped.values()) {
          const transformed = Math.log1p(value);
          maxLog = Math.max(maxLog, transformed);
          transformedSamples.push(transformed);
        }
      }
      transformedSamples.sort((a, b) => a - b);
      const p98 = Math.max(1, percentile(transformedSamples, 0.98));
      const lowCut = thresholdLow * p98;
      const contrastBoost = 3;
      const highExpansion =
        thresholdHigh <= 1
          ? thresholdHigh
          : 1 + (thresholdHigh - 1) * (0.08 / contrastBoost);
      const highCutRaw = Math.min(highExpansion * p98, maxLog * (1 + 0.04 / contrastBoost));
      const highCut = Math.max(lowCut + 0.001, highCutRaw);
      const persisted = new Map<number, number>();
      const decay = 0.92;
      const minPersist = 0.01;

      for (let bar = fromBar; bar <= toBar; bar += 1) {
        const x = barToX(bar);
        const widthPx = Math.max(1, viewport.x_pixels_per_bar + 0.2);
        if (x > layout.dataWidth || x + widthPx < 0) continue;
        const column = heatmapByBarIndex.get(bar);
        const raw = new Map<number, number>();
        if (column) {
          const columnCenterRowRaw = Number(column.center_row);
          const columnCenterRow = isReplayChart && Number.isFinite(columnCenterRowRaw) ? columnCenterRowRaw : anchorRow;
          const anchorGroupedRow = Math.floor(columnCenterRow / rowBinMultiplier);
          for (const cell of column.rows) {
            const groupedRow = Math.floor(cell.row / rowBinMultiplier);
            if (Math.abs(groupedRow - anchorGroupedRow) > effectiveGroupedRangeLimit) continue;
            raw.set(groupedRow, (raw.get(groupedRow) ?? 0) + cell.value);
          }
        }

        const keys = new Set<number>([...persisted.keys(), ...raw.keys()]);
        for (const row of keys) {
          const prev = persisted.get(row) ?? 0;
          const next = raw.has(row) ? Math.max(raw.get(row) ?? 0, prev * decay) : prev * decay;
          if (next <= minPersist) {
            persisted.delete(row);
            continue;
          }
          persisted.set(row, next);
          const columnCenterRowRaw = Number(column?.center_row ?? NaN);
          const columnCenterRow = isReplayChart && Number.isFinite(columnCenterRowRaw) ? columnCenterRowRaw : anchorRow;
          const baseRow = row * rowBinMultiplier;
          const lower = anchorPrice + (baseRow - columnCenterRow) * bucketSize;
          const upper = anchorPrice + (baseRow + rowBinMultiplier - columnCenterRow) * bucketSize;
          const y1 = priceToY(upper);
          const y2 = priceToY(lower);
          const top = Math.min(y1, y2);
          const h = Math.max(1, Math.abs(y2 - y1));
          if (top > layout.plotHeight || top + h < 0) continue;
          const transformed = Math.log1p(next);
          const t = clamp((transformed - lowCut) / (highCut - lowCut), 0, 1);
          const smooth = t * t * (3 - 2 * t);
          const contrastGamma = 1.35 + Math.max(0, thresholdHigh - 1) * (1.25 * contrastBoost);
          const intensity = Math.pow(smooth, contrastGamma);
          ctx.fillStyle = heatmapColor(intensity);
          ctx.fillRect(x, top, widthPx, h);
          drawnHeatCells += 1;
        }
      }
      // Replay fallback: if filtering removes everything, force-draw visible heatmap
      // cells so Show/Hide Heatmap actually shows a background layer.
      if (isReplayChart && drawnHeatCells === 0) {
        ctx.globalAlpha = 0.35;
        let maxRaw = 0;
        for (let bar = fromBar; bar <= toBar; bar += 1) {
          const column = heatmapByBarIndex.get(bar);
          if (!column) continue;
          for (const cell of column.rows) {
            if (Number.isFinite(cell.value)) maxRaw = Math.max(maxRaw, cell.value);
          }
        }
        const denom = Math.max(1, maxRaw);
        for (let bar = fromBar; bar <= toBar; bar += 1) {
          const x = barToX(bar);
          const widthPx = Math.max(1, viewport.x_pixels_per_bar + 0.2);
          if (x > layout.dataWidth || x + widthPx < 0) continue;
          const column = heatmapByBarIndex.get(bar);
          if (!column) continue;
          const columnCenterRowRaw = Number(column.center_row);
          const columnCenterRow = Number.isFinite(columnCenterRowRaw) ? columnCenterRowRaw : anchorRow;
          for (const cell of column.rows) {
            const value = Number(cell.value);
            if (!Number.isFinite(value) || value <= 0) continue;
            const lower = anchorPrice + (cell.row - columnCenterRow) * bucketSize;
            const upper = anchorPrice + (cell.row + 1 - columnCenterRow) * bucketSize;
            const y1 = priceToY(upper);
            const y2 = priceToY(lower);
            const top = Math.min(y1, y2);
            const h = Math.max(1, Math.abs(y2 - y1));
            if (top > layout.plotHeight || top + h < 0) continue;
            const intensity = clamp(value / denom, 0, 1);
            const gamma = Math.pow(intensity, 0.6);
            ctx.fillStyle = heatmapColor(gamma);
            ctx.fillRect(x, top, widthPx, h);
            drawnHeatCells += 1;
          }
        }
      }
      ctx.globalAlpha = prevAlpha;
    }

    for (const candle of indexedCandles) {
      const x0 = barToX(candle.bar_index);
      const x1 = barToX(candle.bar_index + candle.bar_span);
      if (x1 < 0 || x0 > layout.dataWidth) continue;
      const centerX = (x0 + x1) / 2;
      const bodyWidth = clamp(Math.abs(x1 - x0) * 0.62, 1.5, 10);
      const openY = priceToY(candle.open);
      const closeY = priceToY(candle.close);
      const highY = priceToY(candle.high);
      const lowY = priceToY(candle.low);
      const up = candle.close >= candle.open;
      const color = up ? "#02cc86" : "#ef4253";
      ctx.strokeStyle = color;
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.moveTo(centerX, highY);
      ctx.lineTo(centerX, lowY);
      ctx.stroke();
      const bodyTop = Math.min(openY, closeY);
      const bodyHeight = Math.max(1.5, Math.abs(closeY - openY));
      ctx.fillRect(centerX - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);
    }

    // Replay decision markers (trail + current marker).
    const decisionLayer = (snapshot as unknown as { decision_layer?: Record<string, unknown> } | null)?.decision_layer ?? {};
    const replayDecision = decisionLayer["replay_decision"] as Record<string, unknown> | undefined;
    const replayHistoryRaw = Array.isArray(decisionLayer["replay_history"]) ? (decisionLayer["replay_history"] as unknown[]) : [];
    const replayHistory = replayHistoryRaw.filter((item): item is Record<string, unknown> => !!item && typeof item === "object");
      const replayFramesTotal = Number(decisionLayer["replay_frames_total"] ?? 0);
      const replayModeRaw = String(decisionLayer["replay_data_mode"] ?? "").toLowerCase();
      const isPredictionTraceReplay = replayModeRaw.includes("prediction_trace") || replayModeRaw.includes("trace");
      const showReplayMarkers = replayOverlayMode !== "aggregated";
      const showReplayBubbles = replayOverlayMode !== "trace";
      const replayBubbleLayerGlobal = decisionLayer["replay_candle_bubbles_global"];
      const replayBubbleLayerLocal = decisionLayer["replay_candle_bubbles"];
      const hasReplayBubbleLayer =
        (replayBubbleLayerGlobal && typeof replayBubbleLayerGlobal === "object" && Object.keys(replayBubbleLayerGlobal as Record<string, unknown>).length > 0) ||
        (replayBubbleLayerLocal && typeof replayBubbleLayerLocal === "object" && Object.keys(replayBubbleLayerLocal as Record<string, unknown>).length > 0);
      const replayMode =
        replayModeRaw.includes("replay") ||
        replayModeRaw.includes("prediction_trace") ||
      replayModeRaw.includes("trace") ||
      isReplayChart;
    if ((replayDecision || replayHistory.length > 0 || hasReplayBubbleLayer) && indexedCandles.length > 0 && (showReplayMarkers || showReplayBubbles)) {
      const candleByBucket = new Map<number, IndexedCandle>();
      for (const candle of indexedCandles) {
        candleByBucket.set(candle.ts_ms, candle);
      }
      ctx.save();
      if (showReplayMarkers) {
        const markers = replayHistory.length > 0 ? replayHistory : [replayDecision as Record<string, unknown>];
        const markerSampleValues = markers
          .map((m) => Number(m.sample_index ?? m.step_index ?? m.ts_key ?? m.ts_ms ?? NaN))
          .filter((v) => Number.isFinite(v) && v >= 0);
        const markerSampleMax =
          replayFramesTotal > 1
            ? replayFramesTotal - 1
            : markerSampleValues.length > 0
              ? Math.max(...markerSampleValues)
              : Math.max(1, markers.length - 1);
        const maxMarkers = Math.min(markers.length, indexedCandles.length);
        const startMarkerIdx = markers.length - maxMarkers;
        for (let i = 0; i < maxMarkers; i += 1) {
          const marker = markers[startMarkerIdx + i];
          const markerTsRaw = Number(marker.plot_ts_ms ?? marker.ts_ms ?? NaN);
          const markerTs = Number.isFinite(markerTsRaw) ? Math.floor(markerTsRaw / timeframeMs) * timeframeMs : NaN;
          let candle: IndexedCandle | undefined;
          // Real epoch-millisecond timestamp path.
          if (Number.isFinite(markerTs) && markerTs > 946684800000) {
            candle = candleByBucket.get(markerTs);
          }
          // Replay-trace fallback path: ts/sample values are sequence indices, not wall-clock ts.
          if (!candle) {
            const sampleIdx = Number(marker.sample_index ?? marker.step_index ?? marker.ts_key ?? marker.ts_ms ?? NaN);
            if (Number.isFinite(sampleIdx) && sampleIdx >= 0) {
              const normalized = sampleIdx / Math.max(1, markerSampleMax);
              const mappedIdx = Math.max(0, Math.min(indexedCandles.length - 1, Math.floor(normalized * Math.max(0, indexedCandles.length - 1))));
              candle = indexedCandles[mappedIdx];
            }
          }
          if (!candle) candle = indexedCandles[Math.max(0, indexedCandles.length - maxMarkers + i)];
          if (!marker || !candle) continue;
          const actionRaw = String(marker.action ?? marker.predicted_action ?? marker.predicted_label ?? "HOLD").toUpperCase();
          const action =
            actionRaw === "UP" || actionRaw === "BUY"
              ? "LONG"
              : actionRaw === "DOWN" || actionRaw === "SELL"
                ? "SHORT"
                : actionRaw === "LONG" || actionRaw === "SHORT"
                  ? actionRaw
                  : "HOLD";
          const x0 = barToX(candle.bar_index);
          const x1 = barToX(candle.bar_index + candle.bar_span);
          const centerX = (x0 + x1) / 2;
          const highY = priceToY(candle.high);
          const lowY = priceToY(candle.low);
          const markerY =
            action === "LONG"
              ? Math.max(6, lowY + 10)
              : action === "SHORT"
                ? Math.max(6, highY - 10)
                : Math.max(6, (highY + lowY) / 2);
          const isCurrent = i === maxMarkers - 1;
          if (action === "LONG") {
            ctx.fillStyle = isCurrent ? "#00ffb0" : "#20d7a5";
            ctx.strokeStyle = "rgba(0,0,0,0.45)";
            ctx.lineWidth = isCurrent ? 1.4 : 1.1;
            ctx.beginPath();
            ctx.moveTo(centerX, markerY - (isCurrent ? 10 : 7));
            ctx.lineTo(centerX - (isCurrent ? 8 : 6), markerY + (isCurrent ? 7 : 5));
            ctx.lineTo(centerX + (isCurrent ? 8 : 6), markerY + (isCurrent ? 7 : 5));
            ctx.closePath();
            ctx.fill();
            ctx.stroke();
          } else if (action === "SHORT") {
            ctx.fillStyle = isCurrent ? "#ff4b6b" : "#ff6b75";
            ctx.strokeStyle = "rgba(0,0,0,0.45)";
            ctx.lineWidth = isCurrent ? 1.4 : 1.1;
            ctx.beginPath();
            ctx.moveTo(centerX, markerY + (isCurrent ? 10 : 7));
            ctx.lineTo(centerX - (isCurrent ? 8 : 6), markerY - (isCurrent ? 7 : 5));
            ctx.lineTo(centerX + (isCurrent ? 8 : 6), markerY - (isCurrent ? 7 : 5));
            ctx.closePath();
            ctx.fill();
            ctx.stroke();
          } else {
            ctx.fillStyle = isCurrent ? "#ffe88a" : "#ffd35a";
            ctx.beginPath();
            ctx.arc(centerX, markerY, isCurrent ? 5.8 : 4.0, 0, Math.PI * 2);
            ctx.fill();
            ctx.strokeStyle = "rgba(0,0,0,0.45)";
            ctx.lineWidth = 1.1;
            ctx.stroke();
          }
        }
      }

      if (showReplayBubbles) {
        // Replay candle bubble stacks: SHORT/LONG/HOLD counts + avg predicted magnitude (%).
        const replayBubblesGlobalRaw =
        (decisionLayer["replay_candle_bubbles_global"] as Record<
          string,
          {
            short_count?: number;
            long_count?: number;
            hold_count?: number;
            avg_predicted_magnitude_pct?: number;
          }
        > | undefined) ?? {};
        const replayBubblesRaw =
        (decisionLayer["replay_candle_bubbles"] as Record<
          string,
          {
            short_count?: number;
            long_count?: number;
            hold_count?: number;
            avg_predicted_magnitude_pct?: number;
          }
        > | undefined) ?? {};
        const hasGlobalReplayBubbles = Object.keys(replayBubblesGlobalRaw).length > 0;
        const forceReplayBubblesEachCandle = Boolean(decisionLayer["replay_force_bubbles_each_candle"]);
        const resolvedBubblesRaw: Record<
        string,
        {
          short_count?: number;
          long_count?: number;
          hold_count?: number;
          avg_predicted_magnitude_pct?: number;
        }
      > = hasGlobalReplayBubbles ? { ...replayBubblesGlobalRaw } : { ...replayBubblesRaw };
        // In prediction-trace replay, keep a single authoritative source to avoid
        // slider-position-dependent bubble changes.
        if (isPredictionTraceReplay && !hasGlobalReplayBubbles && !forceReplayBubblesEachCandle) {
          for (const k of Object.keys(resolvedBubblesRaw)) {
            delete resolvedBubblesRaw[k];
          }
        }
        if (!isPredictionTraceReplay && !hasGlobalReplayBubbles && replayHistory.length > 0 && indexedCandles.length > 0) {
        // Authoritative fallback: derive per-candle bubbles from replay_history directly.
        // This keeps bubbles consistent with visible replay markers.
        const markerSampleValues = replayHistory
          .map((m) => Number(m.sample_index ?? m.step_index ?? m.ts_key ?? m.ts_ms ?? NaN))
          .filter((v) => Number.isFinite(v) && v >= 0);
        const markerSampleMax =
          replayFramesTotal > 1
            ? replayFramesTotal - 1
            : markerSampleValues.length > 0
              ? Math.max(...markerSampleValues)
              : Math.max(1, replayHistory.length - 1);
        const acc = new Map<number, { short_count: number; long_count: number; hold_count: number; mag_sum: number; mag_n: number }>();
        for (let i = 0; i < replayHistory.length; i += 1) {
          const marker = replayHistory[i];
          const markerTsRaw = Number(marker.plot_ts_ms ?? marker.ts_ms ?? NaN);
          const markerTs = Number.isFinite(markerTsRaw) ? Math.floor(markerTsRaw / timeframeMs) * timeframeMs : NaN;
          let candle: IndexedCandle | undefined;
          if (Number.isFinite(markerTs) && markerTs > 946684800000) {
            candle = indexedCandles.find((c) => c.ts_ms === markerTs);
          }
          if (!candle) {
            const sampleIdx = Number(marker.sample_index ?? marker.step_index ?? marker.ts_key ?? marker.ts_ms ?? NaN);
            const normalized = Number.isFinite(sampleIdx) && sampleIdx >= 0
              ? sampleIdx / Math.max(1, markerSampleMax)
              : i / Math.max(1, replayHistory.length - 1);
            const mappedIdx = Math.max(0, Math.min(indexedCandles.length - 1, Math.floor(normalized * Math.max(0, indexedCandles.length - 1))));
            candle = indexedCandles[mappedIdx];
          }
          if (!candle) continue;
          const minuteTs = Math.floor(Number(candle.ts_ms) / 60_000) * 60_000;
          const row = acc.get(minuteTs) ?? { short_count: 0, long_count: 0, hold_count: 0, mag_sum: 0, mag_n: 0 };
          const actionRaw = String(marker.predicted_label ?? marker.predicted_action ?? marker.action ?? "HOLD").toUpperCase();
          if (actionRaw === "LONG" || actionRaw === "UP" || actionRaw === "BUY") row.long_count += 1;
          else if (actionRaw === "SHORT" || actionRaw === "DOWN" || actionRaw === "SELL") row.short_count += 1;
          else row.hold_count += 1;
          const mag = Number(marker.predicted_magnitude_pct ?? marker.magnitude_pct ?? NaN);
          if (Number.isFinite(mag)) {
            row.mag_sum += mag;
            row.mag_n += 1;
          }
          acc.set(minuteTs, row);
        }
        if (acc.size > 0) {
          for (const [minuteTs, row] of acc.entries()) {
            resolvedBubblesRaw[String(minuteTs)] = {
              short_count: row.short_count,
              long_count: row.long_count,
              hold_count: row.hold_count,
              avg_predicted_magnitude_pct: row.mag_n > 0 ? row.mag_sum / row.mag_n : 0,
            };
          }
        }
        }
        if (!isPredictionTraceReplay && !hasGlobalReplayBubbles && Object.keys(resolvedBubblesRaw).length === 0 && replayDecision && indexedCandles.length > 0) {
        const actionRaw = String(replayDecision.action ?? replayDecision.predicted_action ?? replayDecision.predicted_label ?? "HOLD").toUpperCase();
        const lastCandle = indexedCandles[indexedCandles.length - 1];
        const minuteTs = Math.floor(Number(lastCandle.ts_ms) / 60_000) * 60_000;
        const mag = Number(replayDecision.predicted_magnitude_pct ?? replayDecision.magnitude_pct ?? 0);
        resolvedBubblesRaw[String(minuteTs)] = {
          short_count: actionRaw === "SHORT" || actionRaw === "DOWN" || actionRaw === "SELL" ? 1 : 0,
          long_count: actionRaw === "LONG" || actionRaw === "UP" || actionRaw === "BUY" ? 1 : 0,
          hold_count: actionRaw === "HOLD" ? 1 : 0,
          avg_predicted_magnitude_pct: Number.isFinite(mag) ? mag : 0,
        };
        }
        // Keep replay bubbles deterministic per slider/frame position.
        // Do not persist stale bubble maps across frames.
        lastReplayBubblesRef.current = {};
        if (Object.keys(resolvedBubblesRaw).length > 0) {
        const bubbleRadius = 11;
        const bubbleGap = 5;
        const stackSpacing = bubbleRadius * 2 + bubbleGap;
        const topWallY = bubbleRadius + 4;
        const bubbleByMinute = new Map<
          number,
          {
            short_count: number;
            long_count: number;
            hold_count: number;
            avg_predicted_magnitude_pct: number;
            _mag_count: number;
          }
        >();
        for (const [tsKey, value] of Object.entries(resolvedBubblesRaw)) {
          const minuteTs = Number(tsKey);
          if (!Number.isFinite(minuteTs)) continue;
          const shortCount = Math.max(0, Number(value?.short_count ?? 0));
          const longCount = Math.max(0, Number(value?.long_count ?? 0));
          const holdCount = Math.max(0, Number(value?.hold_count ?? 0));
          const avgMag = Number(value?.avg_predicted_magnitude_pct ?? 0);
          const magCount = shortCount + longCount + holdCount;
          bubbleByMinute.set(minuteTs, {
            short_count: shortCount,
            long_count: longCount,
            hold_count: holdCount,
            avg_predicted_magnitude_pct: Number.isFinite(avgMag) ? avgMag : 0,
            _mag_count: Math.max(0, magCount),
          });
        }
        // Final fallback: when timestamp mapping misses chart candles, aggregate by replay sample index
        // onto candle indices so bubbles stay attached to visible candles.
        if (!isPredictionTraceReplay && bubbleByMinute.size === 0 && replayHistory.length > 0 && indexedCandles.length > 0) {
          const markerSampleValues = replayHistory
            .map((m) => Number(m.sample_index ?? m.step_index ?? m.ts_key ?? m.ts_ms ?? NaN))
            .filter((v) => Number.isFinite(v) && v >= 0);
          const markerSampleMax =
            replayFramesTotal > 1
              ? replayFramesTotal - 1
              : markerSampleValues.length > 0
                ? Math.max(...markerSampleValues)
                : Math.max(1, replayHistory.length - 1);
          const accByMinute = new Map<number, { short_count: number; long_count: number; hold_count: number; mag_sum: number; mag_n: number }>();
          for (let i = 0; i < replayHistory.length; i += 1) {
            const marker = replayHistory[i];
            const sampleIdx = Number(marker.sample_index ?? marker.step_index ?? marker.ts_key ?? marker.ts_ms ?? NaN);
            const normalized = Number.isFinite(sampleIdx) && sampleIdx >= 0
              ? sampleIdx / Math.max(1, markerSampleMax)
              : i / Math.max(1, replayHistory.length - 1);
            const mappedIdx = Math.max(0, Math.min(indexedCandles.length - 1, Math.floor(normalized * Math.max(0, indexedCandles.length - 1))));
            const candle = indexedCandles[mappedIdx];
            if (!candle) continue;
            const minuteTs = Math.floor(Number(candle.ts_ms) / 60_000) * 60_000;
            const row = accByMinute.get(minuteTs) ?? { short_count: 0, long_count: 0, hold_count: 0, mag_sum: 0, mag_n: 0 };
            const actionRaw = String(marker.predicted_label ?? marker.predicted_action ?? marker.action ?? "HOLD").toUpperCase();
            if (actionRaw === "LONG" || actionRaw === "UP" || actionRaw === "BUY") row.long_count += 1;
            else if (actionRaw === "SHORT" || actionRaw === "DOWN" || actionRaw === "SELL") row.short_count += 1;
            else row.hold_count += 1;
            const mag = Number(marker.predicted_magnitude_pct ?? marker.magnitude_pct ?? NaN);
            if (Number.isFinite(mag)) {
              row.mag_sum += mag;
              row.mag_n += 1;
            }
            accByMinute.set(minuteTs, row);
          }
          for (const [minuteTs, row] of accByMinute.entries()) {
            const total = row.short_count + row.long_count + row.hold_count;
            bubbleByMinute.set(minuteTs, {
              short_count: row.short_count,
              long_count: row.long_count,
              hold_count: row.hold_count,
              avg_predicted_magnitude_pct: row.mag_n > 0 ? row.mag_sum / row.mag_n : 0,
              _mag_count: Math.max(total, row.mag_n),
            });
          }
        }

        // If global trace bubbles exist but timestamps don't overlap the visible candle window,
        // remap them deterministically by order so replay still shows meaningful counts.
        if (
          isPredictionTraceReplay &&
          hasGlobalReplayBubbles &&
          bubbleByMinute.size > 0 &&
          indexedCandles.length > 0
        ) {
          let overlap = 0;
          for (const candle of indexedCandles) {
            const minuteTs = Math.floor(Number(candle.ts_ms) / 60_000) * 60_000;
            if (bubbleByMinute.has(minuteTs)) overlap += 1;
          }
          if (overlap === 0) {
            const sortedEntries = Array.from(bubbleByMinute.entries()).sort((a, b) => a[0] - b[0]);
            const remapped = new Map<
              number,
              {
                short_count: number;
                long_count: number;
                hold_count: number;
                avg_predicted_magnitude_pct: number;
                _mag_count: number;
              }
            >();
            const lastIdx = Math.max(0, indexedCandles.length - 1);
            const denom = Math.max(1, sortedEntries.length - 1);
            for (let i = 0; i < sortedEntries.length; i += 1) {
              const ratio = i / denom;
              const mappedIdx = Math.max(0, Math.min(lastIdx, Math.round(ratio * lastIdx)));
              const c = indexedCandles[mappedIdx];
              if (!c) continue;
              const minuteTs = Math.floor(Number(c.ts_ms) / 60_000) * 60_000;
              remapped.set(minuteTs, sortedEntries[i][1]);
            }
            bubbleByMinute.clear();
            for (const [k, v] of remapped.entries()) bubbleByMinute.set(k, v);
          }
        }

          for (const candle of indexedCandles) {
          const candleStart = Math.floor(Number(candle.ts_ms) / 60_000) * 60_000;
          const candleEnd = candleStart + Math.max(60_000, timeframeMs);
          let red = 0;
          let green = 0;
          let yellow = 0;
          let magWeightedSum = 0;
          let magCount = 0;
          for (let t = candleStart; t < candleEnd; t += 60_000) {
            const row = bubbleByMinute.get(t);
            if (!row) continue;
            red += row.short_count;
            green += row.long_count;
            yellow += row.hold_count;
            const rowCount = row._mag_count;
            if (rowCount > 0) {
              magWeightedSum += row.avg_predicted_magnitude_pct * rowCount;
              magCount += rowCount;
            }
          }
          const blue = magCount > 0 ? magWeightedSum / magCount : 0;
          const hasData = red > 0 || green > 0 || yellow > 0 || magCount > 0;
          if (!hasData && !forceReplayBubblesEachCandle) continue;

          const x0 = barToX(candle.bar_index);
          const x1 = barToX(candle.bar_index + candle.bar_span);
          const centerX = (x0 + x1) / 2;
          if (centerX < -18 || centerX > layout.plotWidth + 18) continue;
          const stackX = centerX;

          const drawBubble = (y: number, color: string, text: string, darkText = false) => {
            ctx.fillStyle = color;
            ctx.beginPath();
            ctx.arc(stackX, y, bubbleRadius, 0, Math.PI * 2);
            ctx.fill();
            ctx.strokeStyle = "rgba(0,0,0,0.45)";
            ctx.lineWidth = 1;
            ctx.stroke();
            ctx.fillStyle = darkText ? "rgba(20,20,20,0.95)" : "#ffffff";
            ctx.font = "bold 10px sans-serif";
            ctx.textAlign = "center";
            ctx.textBaseline = "middle";
            ctx.fillText(text, stackX, y + 0.5);
          };

          const yRed = topWallY;
          const yGreen = yRed + stackSpacing;
          const yYellow = yGreen + stackSpacing;
          const yBlue = yYellow + stackSpacing;

          drawBubble(yRed, "#ef4253", `${Math.max(0, Math.round(red))}`);
          drawBubble(yGreen, "#02cc86", `${Math.max(0, Math.round(green))}`);
          drawBubble(yYellow, "#ffe066", `${Math.max(0, Math.round(yellow))}`, true);
          const avgText = `${Number.isFinite(blue) ? blue.toFixed(2) : "0.00"}`;
          drawBubble(yBlue, "#3b82f6", avgText);
          }
        }
      }

      ctx.restore();
    }

    if (overlayLines.length > 0) {
      for (const line of overlayLines) {
        if (!Number.isFinite(line.price)) continue;
        const y = priceToY(line.price);
        if (y < 0 || y > layout.plotHeight) continue;
        ctx.setLineDash([]);
        ctx.strokeStyle = line.color;
        ctx.lineWidth = line.color === "#111111" ? 1.9 : 1.4;
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(layout.plotWidth, y);
        ctx.stroke();
      }
      ctx.lineWidth = 1;
    }

    let activeRulerPanel: { lines: string[]; color: string } | null = null;
    const renderDrawings: ChartDrawing[] = [...drawings];
    if (rectDraft) renderDrawings.push(rectDraft);
    if (rulerDraft) renderDrawings.push(rulerDraft);
    if (renderDrawings.length > 0) {
      for (const drawing of renderDrawings) {
        const isActive = activeDrawingId !== null && drawing.id === activeDrawingId;
        if (drawing.kind === "hline") {
          const y = priceToY(drawing.price);
          if (y < 0 || y > layout.plotHeight) continue;
          ctx.setLineDash([]);
          ctx.strokeStyle = drawing.color;
          ctx.lineWidth = isActive ? 2.2 : 1.5;
          ctx.beginPath();
          ctx.moveTo(0, y);
          ctx.lineTo(layout.plotWidth, y);
          ctx.stroke();
          continue;
        }
        if (drawing.kind === "ruler") {
          const xStart = barToX(drawing.bar_start);
          const yStart = priceToY(drawing.price_start);
          const xEnd = barToX(drawing.bar_end);
          const yEnd = priceToY(drawing.price_end);
          const move = drawing.price_end - drawing.price_start;
          const pct = drawing.price_start !== 0 ? (move / drawing.price_start) * 100 : 0;
          const bars = Math.max(0, Math.round(Math.abs(drawing.bar_end - drawing.bar_start)));
          const timeMs = bars * timeframeMs;
          const color = move > 0 ? "#02cc86" : move < 0 ? "#ef4253" : "#8aa0bc";
          ctx.setLineDash([]);
          ctx.strokeStyle = color;
          ctx.fillStyle = color;
          ctx.lineWidth = isActive ? 2.2 : 1.5;
          ctx.beginPath();
          ctx.moveTo(xStart, yStart);
          ctx.lineTo(xEnd, yEnd);
          ctx.stroke();
          const angle = Math.atan2(yEnd - yStart, xEnd - xStart);
          const head = 8;
          ctx.beginPath();
          ctx.moveTo(xEnd, yEnd);
          ctx.lineTo(xEnd - head * Math.cos(angle - Math.PI / 6), yEnd - head * Math.sin(angle - Math.PI / 6));
          ctx.lineTo(xEnd - head * Math.cos(angle + Math.PI / 6), yEnd - head * Math.sin(angle + Math.PI / 6));
          ctx.closePath();
          ctx.fill();

          const replayCoreByTs =
            decisionLayer["replay_candle_core_by_ts"] && typeof decisionLayer["replay_candle_core_by_ts"] === "object"
              ? (decisionLayer["replay_candle_core_by_ts"] as Record<string, unknown>)
              : null;
          let predPct: number | null = null;
          if (replayMode && replayCoreByTs) {
            const nearestEnd = indexedCandles.reduce<IndexedCandle | null>((best, candle) => {
              if (!best) return candle;
              return Math.abs(candle.bar_index - drawing.bar_end) < Math.abs(best.bar_index - drawing.bar_end) ? candle : best;
            }, null);
            if (nearestEnd) {
              const key = String(Math.floor(nearestEnd.ts_ms / 60_000) * 60_000);
              const row = replayCoreByTs[key];
              if (row && typeof row === "object") {
                const value = Number(
                  (row as Record<string, unknown>).predicted_magnitude_pct ??
                    (row as Record<string, unknown>).magnitude_pct ??
                    NaN
                );
                if (Number.isFinite(value)) predPct = value;
              }
            }
          }

          const lines = [
            `From: ${formatPrice(drawing.price_start)}`,
            `To: ${formatPrice(drawing.price_end)}`,
            `Move: ${formatSigned(move, 4)}`,
            `Move %: ${formatSigned(pct, 2)}%`,
            `Bars: ${bars}`,
            `Time: ${formatDurationCompact(timeMs)}`,
          ];
          if (predPct !== null) {
            lines.push(`Predicted %: ${formatSigned(predPct, 2)}%`);
            lines.push(`Error: ${formatSigned(pct - predPct, 2)}%`);
          }
          if (isActive) {
            activeRulerPanel = { lines, color };
          }
          continue;
        }

        const left = barToX(drawing.bar_start);
        const right = barToX(drawing.bar_end);
        const top = priceToY(drawing.price_high);
        const bottom = priceToY(drawing.price_low);
        const x = Math.min(left, right);
        const y = Math.min(top, bottom);
        const w = Math.max(1, Math.abs(right - left));
        const h = Math.max(1, Math.abs(bottom - top));
        ctx.fillStyle = "rgba(46, 137, 255, 0.14)";
        ctx.strokeStyle = drawing.color;
        ctx.lineWidth = isActive ? 2 : 1.2;
        ctx.fillRect(x, y, w, h);
        ctx.strokeRect(x, y, w, h);

        if (isActive) {
          const handleSize = 6;
          const handles = [
            { x, y },
            { x: x + w, y },
            { x, y: y + h },
            { x: x + w, y: y + h },
          ];
          ctx.fillStyle = "#ffffff";
          for (const handle of handles) {
            ctx.fillRect(handle.x - handleSize / 2, handle.y - handleSize / 2, handleSize, handleSize);
            ctx.strokeStyle = drawing.color;
            ctx.strokeRect(handle.x - handleSize / 2, handle.y - handleSize / 2, handleSize, handleSize);
          }
        }
      }
      ctx.lineWidth = 1;
    }

    if (activeRulerPanel) {
      const lines = activeRulerPanel.lines;
      ctx.font = "11px Segoe UI";
      const padding = 7;
      const lineH = 14;
      const panelWidth = Math.max(...lines.map((line) => ctx.measureText(line).width)) + padding * 2;
      const panelHeight = lines.length * lineH + padding * 2;
      const panelX = clamp(layout.plotWidth - panelWidth - 10, 6, layout.plotWidth - panelWidth - 6);
      const panelY = clamp(layout.plotHeight - panelHeight - 10, 6, layout.plotHeight - panelHeight - 6);
      ctx.fillStyle = "rgba(10, 18, 29, 0.92)";
      ctx.fillRect(panelX, panelY, panelWidth, panelHeight);
      ctx.strokeStyle = activeRulerPanel.color;
      ctx.lineWidth = 1;
      ctx.strokeRect(panelX, panelY, panelWidth, panelHeight);
      ctx.fillStyle = "#d6e1ee";
      for (let i = 0; i < lines.length; i += 1) {
        ctx.fillText(lines[i], panelX + padding, panelY + padding + (i + 1) * lineH - 4);
      }
      ctx.lineWidth = 1;
    }

    const currentPrice = snapshot?.last_trade_price || snapshot?.mark_price || 0;
    if (currentPrice > 0) {
      const y = priceToY(currentPrice);
      ctx.setLineDash([6, 6]);
      ctx.strokeStyle = "#08d47c";
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(layout.plotWidth, y);
      ctx.stroke();
      ctx.setLineDash([]);
      const currentPriceLabel = formatPrice(currentPrice);
      ctx.font = "9px Segoe UI";
      const currentPriceTagWidth = clamp(Math.ceil(ctx.measureText(currentPriceLabel).width) + 8, 30, layout.axisWidth - 6);
      const currentPriceTagX = layout.plotWidth + layout.axisWidth - currentPriceTagWidth - 3;
      ctx.fillStyle = "#0dbf7d";
      ctx.fillRect(currentPriceTagX, y - 6, currentPriceTagWidth, 12);
      ctx.fillStyle = "#fff";
      ctx.fillText(currentPriceLabel, currentPriceTagX + 4, y + 3);
    }

    const rawStep = yRange / 8;
    const computedStep = niceStep(rawStep);
    const previous = axisStepRef.current;
    let step = computedStep;
    if (previous > 0 && computedStep >= previous * 0.6 && computedStep <= previous * 1.8) {
      step = previous;
    } else {
      axisStepRef.current = computedStep;
      step = computedStep;
    }
    ctx.font = "11px Segoe UI";
    ctx.fillStyle = "#5f6672";
    ctx.strokeStyle = "rgba(0,0,0,0.06)";
    const firstTick = Math.ceil(yMin / step) * step;
    for (let value = firstTick; value <= yMax; value += step) {
      const y = priceToY(value);
      if (y < 0 || y > layout.plotHeight) continue;
      ctx.beginPath();
      ctx.moveTo(layout.plotWidth - 4, y + 0.5);
      ctx.lineTo(layout.plotWidth, y + 0.5);
      ctx.stroke();
      ctx.fillText(formatPrice(value), layout.plotWidth + 6, y + 4);
    }

    if (interaction.crosshair_x !== null && interaction.crosshair_y !== null) {
      const x = interaction.crosshair_x;
      const y = interaction.crosshair_y;
      ctx.setLineDash([4, 4]);
      ctx.strokeStyle = "rgba(255,255,255,0.85)";
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, layout.plotHeight);
      ctx.moveTo(0, y);
      ctx.lineTo(layout.plotWidth, y);
      ctx.stroke();
      ctx.setLineDash([]);

      if (interaction.active_price !== null) {
        const activePriceLabel = formatPrice(interaction.active_price);
        ctx.font = "9px Segoe UI";
        const activePriceTagWidth = clamp(Math.ceil(ctx.measureText(activePriceLabel).width) + 8, 30, layout.axisWidth - 6);
        const activePriceTagX = layout.plotWidth + layout.axisWidth - activePriceTagWidth - 3;
        ctx.fillStyle = "#111";
        ctx.fillRect(activePriceTagX, y - 6, activePriceTagWidth, 12);
        ctx.fillStyle = "#fff";
        ctx.fillText(activePriceLabel, activePriceTagX + 4, y + 3);
      }
    }

    if (interaction.active_bar_index !== null) {
      const nearest = indexedCandles.reduce<IndexedCandle | null>((best, candle) => {
        if (!best) return candle;
        const dBest = Math.abs(best.bar_index - interaction.active_bar_index!);
        const dCur = Math.abs(candle.bar_index - interaction.active_bar_index!);
        return dCur < dBest ? candle : best;
      }, null);
      if (nearest) {
        const ohlcLabel =
          `O ${formatPrice(nearest.open)} H ${formatPrice(nearest.high)} L ${formatPrice(nearest.low)} C ${formatPrice(nearest.close)}`;
        const timeLabel = new Date(nearest.ts_ms).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        });
        ctx.font = "8px Segoe UI";
        const timeWidth = ctx.measureText(timeLabel).width;
        ctx.font = "9px Segoe UI";
        const ohlcWidth = ctx.measureText(ohlcLabel).width;
        const tipWidth = clamp(Math.ceil(Math.max(timeWidth, ohlcWidth)) + 12, 116, 186);
        ctx.fillStyle = "rgba(14, 20, 27, 0.86)";
        ctx.fillRect(8, 8, tipWidth, 28);
        ctx.fillStyle = "#d5dde7";
        ctx.font = "8px Segoe UI";
        ctx.fillText(timeLabel, 14, 18);
        ctx.font = "9px Segoe UI";
        ctx.fillText(ohlcLabel, 14, 29);
      }
    }
  }, [
    bucketSize,
    firstVisibleBar,
    hasHeatmapData,
    heatmapByBarIndex,
    indexedCandles,
    interaction.active_bar_index,
    interaction.active_price,
    interaction.crosshair_x,
    interaction.crosshair_y,
    lastVisibleBar,
    layout.axisWidth,
    layout.dataWidth,
    layout.gapWidth,
    layout.plotHeight,
    layout.plotWidth,
    size.height,
    size.width,
    snapshot?.last_trade_price,
    snapshot?.mark_price,
    thresholdHigh,
    thresholdLow,
    rowSizeMultiplier,
    heatmapCoverageScale,
    overlayLines,
    drawings,
    rectDraft,
    rulerDraft,
    activeDrawingId,
    replayOverlayMode,
    viewport.x_pixels_per_bar,
    timeframeMs,
    yMax,
    yMin,
    yRange,
  ]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const onWheel = (event: WheelEvent) => {
      event.preventDefault();
      event.stopPropagation();
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      if (x < 0 || y < 0 || x > rect.width || y > rect.height) return;

      const modeFactor = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? rect.height : 1;
      const deltaX = event.deltaX * modeFactor;
      const deltaY = event.deltaY * modeFactor;
      const isPinchZoom = event.ctrlKey;
      const absX = Math.abs(deltaX);
      const absY = Math.abs(deltaY);

      if (x <= layout.plotWidth) {
        const canPanWithTouchpad = !isPinchZoom && absX > absY * 1.1;
        if (canPanWithTouchpad) {
          setViewport((prev) => {
            const barsDelta = deltaX / Math.max(MIN_X_PIXELS_PER_BAR, prev.x_pixels_per_bar);
            const nextOffset = Math.max(0, prev.x_offset_bars + barsDelta);
            return {
              ...prev,
              follow_live: nextOffset <= 0.001,
              x_offset_bars: nextOffset <= 0.001 ? 0 : nextOffset,
            };
          });
          return;
        }

        setViewport((prev) => {
          const oldPixels = prev.x_pixels_per_bar;
          const sensitivity = isPinchZoom ? 0.00105 : 0.00085;
          const zoomFactor = Math.exp(-deltaY * sensitivity);
          const boundedFactor = clamp(zoomFactor, 0.9, 1.11);
          const newPixels = clamp(oldPixels * boundedFactor, MIN_X_PIXELS_PER_BAR, MAX_X_PIXELS_PER_BAR);
          if (Math.abs(newPixels - oldPixels) < 0.0001) return prev;

          const oldRightPadding = prev.right_padding_bars;
          const oldVisibleBars = layout.plotWidth / oldPixels;
          const oldLiveFirst = totalBars - (oldVisibleBars - oldRightPadding);
          const oldFirst = prev.follow_live ? oldLiveFirst : oldLiveFirst - prev.x_offset_bars;

          const anchorX = clamp(x, 0, layout.plotWidth);
          const anchorBar = oldFirst + anchorX / oldPixels;

          const newRightPadding = layout.gapWidth / newPixels;
          const newVisibleBars = layout.plotWidth / newPixels;
          const newLiveFirst = totalBars - (newVisibleBars - newRightPadding);
          const desiredFirst = anchorBar - anchorX / newPixels;
          const newOffset = Math.max(0, newLiveFirst - desiredFirst);
          const userDetached = !prev.follow_live || prev.x_offset_bars > 0.001;
          const followLive = userDetached ? false : newOffset <= 0.001;
          const nextOffset = followLive ? 0 : newOffset;

          return {
            ...prev,
            x_pixels_per_bar: newPixels,
            right_padding_bars: newRightPadding,
            follow_live: followLive,
            x_offset_bars: nextOffset,
          };
        });
        return;
      }

      const { min, max } = yRangeRef.current;
      const oldRange = Math.max(0.0000001, max - min);
      const zoomFactor = Math.exp(deltaY * 0.001);
      const boundedFactor = clamp(zoomFactor, 0.9, 1.11);
      const newRange = oldRange * boundedFactor;
      const ratio = clamp(y / layout.plotHeight, 0, 1);
      const anchorPrice = max - ratio * oldRange;
      const nextYMax = anchorPrice + ratio * newRange;
      const nextYMin = nextYMax - newRange;
      setViewport((prev) => ({
        ...prev,
        y_mode: "manual",
        y_min: nextYMin,
        y_max: nextYMax,
      }));
    };
    canvas.addEventListener("wheel", onWheel, { passive: false });
    return () => canvas.removeEventListener("wheel", onWheel);
  }, [layout.dataWidth, layout.gapWidth, layout.plotHeight, layout.plotWidth, totalBars]);

  useEffect(() => {
    return () => {
      if (rafPointerRef.current !== null) {
        cancelAnimationFrame(rafPointerRef.current);
        rafPointerRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Delete") return;
      if (!activeDrawingId) return;

      const target = event.target as HTMLElement | null;
      if (target) {
        const tagName = target.tagName;
        if (
          tagName === "INPUT" ||
          tagName === "TEXTAREA" ||
          tagName === "SELECT" ||
          target.isContentEditable
        ) {
          return;
        }
      }

      event.preventDefault();
      setDrawings((prev) => prev.filter((drawing) => drawing.id !== activeDrawingId));
      setActiveDrawingId(null);
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [activeDrawingId]);

  function onPointerDown(event: React.PointerEvent<HTMLCanvasElement>) {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const inPlot = x <= layout.plotWidth;
    const pointerData = pointerToBarPrice(x, y);

    if (inPlot) {
      const hit = findDrawingHit(x, y);
      if (hit) {
        setActiveDrawingId(hit.drawing_id);
        if (hit.action === "hline_drag") {
          drawingDragRef.current = { kind: "hline_drag", drawing_id: hit.drawing_id };
        } else if (hit.action === "ruler_move") {
          const origin = drawings.find((item) => item.id === hit.drawing_id);
          if (origin && origin.kind === "ruler") {
            drawingDragRef.current = {
              kind: "ruler_move",
              drawing_id: hit.drawing_id,
              start_bar: pointerData.bar,
              start_price: pointerData.price,
              origin,
            };
          }
        } else {
          const origin = drawings.find((item) => item.id === hit.drawing_id);
          if (origin && origin.kind === "rect") {
            drawingDragRef.current = {
              kind: hit.action,
              drawing_id: hit.drawing_id,
              start_bar: pointerData.bar,
              start_price: pointerData.price,
              origin,
            };
          }
        }
        if (drawingDragRef.current) {
          setInteraction((prev) => ({
            ...prev,
            is_dragging: true,
            drag_target: "plot",
          }));
          updateCrosshair(x, y);
          event.currentTarget.setPointerCapture(event.pointerId);
          return;
        }
      }

      if (drawMode === "hline") {
        const newLine: HLineDrawing = {
          id: nextDrawingId(),
          kind: "hline",
          price: pointerData.price,
          color: "#111111",
        };
        setDrawings((prev) => [...prev, newLine]);
        setActiveDrawingId(newLine.id);
        drawingDragRef.current = {
          kind: "hline_drag",
          drawing_id: newLine.id,
        };
        setInteraction((prev) => ({
          ...prev,
          is_dragging: true,
          drag_target: "plot",
        }));
        updateCrosshair(x, y);
        event.currentTarget.setPointerCapture(event.pointerId);
        return;
      }

      if (drawMode === "rect") {
        setActiveDrawingId(null);
        setRectDraft({
          ...normalizeRect(pointerData.bar, pointerData.price, pointerData.bar, pointerData.price),
          id: "__draft__",
        });
        drawingDragRef.current = {
          kind: "rect_create",
          anchor_bar: pointerData.bar,
          anchor_price: pointerData.price,
        };
        setInteraction((prev) => ({
          ...prev,
          is_dragging: true,
          drag_target: "plot",
        }));
        updateCrosshair(x, y);
        event.currentTarget.setPointerCapture(event.pointerId);
        return;
      }
      if (drawMode === "ruler") {
        const snappedStart = snapPriceForBar(pointerData.bar, pointerData.price);
        setActiveDrawingId(null);
        setRulerDraft({
          id: "__ruler_draft__",
          kind: "ruler",
          bar_start: pointerData.bar,
          bar_end: pointerData.bar,
          price_start: snappedStart,
          price_end: snappedStart,
          color: "#2e89ff",
        });
        drawingDragRef.current = {
          kind: "ruler_create",
          anchor_bar: pointerData.bar,
          anchor_price: snappedStart,
        };
        setInteraction((prev) => ({
          ...prev,
          is_dragging: true,
          drag_target: "plot",
        }));
        updateCrosshair(x, y);
        event.currentTarget.setPointerCapture(event.pointerId);
        return;
      }
      setActiveDrawingId(null);
    }

    const target: "plot" | "price_axis" | null = x <= layout.plotWidth ? "plot" : "price_axis";
    if (!target) return;
    dragRef.current = {
      active: true,
      target,
      last_x: x,
      last_y: y,
    };
    setInteraction((prev) => ({
      ...prev,
      is_dragging: true,
      drag_target: target,
    }));
    updateCrosshair(x, y);
    event.currentTarget.setPointerCapture(event.pointerId);
  }

  function onPointerMove(event: React.PointerEvent<HTMLCanvasElement>) {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    pendingPointerRef.current = { x, y };
    if (rafPointerRef.current !== null) return;
    rafPointerRef.current = requestAnimationFrame(() => {
      rafPointerRef.current = null;
      const pending = pendingPointerRef.current;
      if (!pending) return;
      updateCrosshair(pending.x, pending.y);
      const pointerData = pointerToBarPrice(pending.x, pending.y);

      if (drawingDragRef.current) {
        const drag = drawingDragRef.current;
        if (drag.kind === "hline_drag") {
          setDrawings((prev) =>
            prev.map((item) =>
              item.id === drag.drawing_id && item.kind === "hline"
                ? {
                    ...item,
                    price: pointerData.price,
                  }
                : item
            )
          );
          return;
        }

        if (drag.kind === "rect_create") {
          setRectDraft({
            ...normalizeRect(drag.anchor_bar, drag.anchor_price, pointerData.bar, pointerData.price),
            id: "__draft__",
          });
          return;
        }
        if (drag.kind === "ruler_create") {
          const snapped = snapPriceForBar(pointerData.bar, pointerData.price);
          setRulerDraft({
            id: "__ruler_draft__",
            kind: "ruler",
            bar_start: drag.anchor_bar,
            bar_end: pointerData.bar,
            price_start: drag.anchor_price,
            price_end: snapped,
            color: "#2e89ff",
          });
          return;
        }
        if (drag.kind === "ruler_move") {
          const deltaBar = pointerData.bar - drag.start_bar;
          const deltaPrice = pointerData.price - drag.start_price;
          setDrawings((prev) =>
            prev.map((item) =>
              item.id === drag.drawing_id && item.kind === "ruler"
                ? {
                    ...item,
                    bar_start: drag.origin.bar_start + deltaBar,
                    bar_end: drag.origin.bar_end + deltaBar,
                    price_start: drag.origin.price_start + deltaPrice,
                    price_end: drag.origin.price_end + deltaPrice,
                  }
                : item
            )
          );
          return;
        }

        setDrawings((prev) =>
          prev.map((item) => {
            if (item.id !== drag.drawing_id || item.kind !== "rect") return item;
            const deltaBar = pointerData.bar - drag.start_bar;
            const deltaPrice = pointerData.price - drag.start_price;
            if (drag.kind === "rect_move") {
              return {
                ...item,
                bar_start: drag.origin.bar_start + deltaBar,
                bar_end: drag.origin.bar_end + deltaBar,
                price_low: drag.origin.price_low + deltaPrice,
                price_high: drag.origin.price_high + deltaPrice,
              };
            }
            if (drag.kind === "rect_left") {
              const nextLeft = Math.min(pointerData.bar, drag.origin.bar_end - 0.2);
              return { ...item, bar_start: nextLeft };
            }
            if (drag.kind === "rect_right") {
              const nextRight = Math.max(pointerData.bar, drag.origin.bar_start + 0.2);
              return { ...item, bar_end: nextRight };
            }
            if (drag.kind === "rect_top") {
              const nextTop = Math.max(pointerData.price, drag.origin.price_low + 0.0000001);
              return { ...item, price_high: nextTop };
            }
            const nextBottom = Math.min(pointerData.price, drag.origin.price_high - 0.0000001);
            return { ...item, price_low: nextBottom };
          })
        );
        return;
      }

      if (!dragRef.current.active || !dragRef.current.target) return;
      const dx = pending.x - dragRef.current.last_x;
      const dy = pending.y - dragRef.current.last_y;
      dragRef.current.last_x = pending.x;
      dragRef.current.last_y = pending.y;

      if (dragRef.current.target === "plot") {
        setViewport((prev) => {
          const nextOffset = Math.max(0, prev.x_offset_bars + dx / Math.max(MIN_X_PIXELS_PER_BAR, prev.x_pixels_per_bar));
          if (prev.y_mode !== "manual") {
            return {
              ...prev,
              follow_live: nextOffset <= 0.001,
              x_offset_bars: nextOffset <= 0.001 ? 0 : nextOffset,
            };
          }
          const currentRange = Math.max(0.0000001, prev.y_max - prev.y_min);
          const deltaPrice = (dy / Math.max(1, layout.plotHeight)) * currentRange;
          return {
            ...prev,
            follow_live: nextOffset <= 0.001,
            x_offset_bars: nextOffset <= 0.001 ? 0 : nextOffset,
            y_min: prev.y_min + deltaPrice,
            y_max: prev.y_max + deltaPrice,
          };
        });
        return;
      }

      setViewport((prev) => {
        const currentMin = prev.y_mode === "auto" ? autoRange.min : prev.y_min;
        const currentMax = prev.y_mode === "auto" ? autoRange.max : prev.y_max;
        const currentRange = Math.max(0.0000001, currentMax - currentMin);
        const deltaPrice = (dy / Math.max(1, layout.plotHeight)) * currentRange;
        return {
          ...prev,
          y_mode: "manual",
          y_min: currentMin + deltaPrice,
          y_max: currentMax + deltaPrice,
        };
      });
    });
  }

  function onCanvasClick() {
    if (drawMode !== "none") return;
    if (!isReplayChart) return;
    if (lockedReplayCandleTs !== null) {
      setLockedReplayCandleTs(null);
      return;
    }
    setLockedReplayCandleTs(nearestCandleTsForBar(interaction.active_bar_index));
  }

  function onPointerUp() {
    if (drawingDragRef.current?.kind === "rect_create" && rectDraft) {
      const widthBars = Math.abs(rectDraft.bar_end - rectDraft.bar_start);
      const heightPrice = Math.abs(rectDraft.price_high - rectDraft.price_low);
      if (widthBars >= 0.15 && heightPrice > 0.00000001) {
        const finalized: RectDrawing = {
          ...rectDraft,
          id: nextDrawingId(),
        };
        setDrawings((prev) => [...prev, finalized]);
        setActiveDrawingId(finalized.id);
      }
      setRectDraft(null);
    }
    if (drawingDragRef.current?.kind === "ruler_create" && rulerDraft) {
      const widthBars = Math.abs(rulerDraft.bar_end - rulerDraft.bar_start);
      const heightPrice = Math.abs(rulerDraft.price_end - rulerDraft.price_start);
      if (widthBars >= 0.1 || heightPrice > 0.00000001) {
        const finalized: RulerDrawing = {
          ...rulerDraft,
          id: nextDrawingId(),
        };
        setDrawings((prev) => [...prev, finalized]);
        setActiveDrawingId(finalized.id);
      }
      setRulerDraft(null);
    }
    drawingDragRef.current = null;
    dragRef.current.active = false;
    dragRef.current.target = null;
    setInteraction((prev) => ({
      ...prev,
      is_dragging: false,
      drag_target: null,
    }));
  }

  function onDoubleClick(event: React.MouseEvent<HTMLCanvasElement>) {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    if (x <= layout.plotWidth) {
      resetHorizontal();
      return;
    }
    resetYAuto();
  }

  function onContextMenu(event: React.MouseEvent<HTMLCanvasElement>) {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const hit = findDrawingHit(x, y);
    if (!hit) return;
    event.preventDefault();
    setDrawings((prev) => prev.filter((item) => item.id !== hit.drawing_id));
    if (activeDrawingId === hit.drawing_id) setActiveDrawingId(null);
  }

  return (
    <section ref={panelRef} className="panel orderflow-panel">
      <div className={`heatmap-toolbar${isReplayChart ? " replay-mode" : ""}`}>
        <div className="heatmap-toolbar-left">
          <div className="heatmap-toolbar-item">
            <span>Low</span>
            <input type="range" min={0} max={1} step={0.01} value={thresholdLow} onChange={(e) => setThresholdLow(Number(e.target.value))} />
          </div>
          <div className="heatmap-toolbar-item">
            <span>High</span>
            <input type="range" min={0} max={3} step={0.01} value={thresholdHigh} onChange={(e) => setThresholdHigh(Number(e.target.value))} />
            <span>{thresholdHigh.toFixed(2)}x</span>
          </div>
          <div className="heatmap-toolbar-item heatmap-row-size-item">
            <span>Row Size</span>
            <input
              type="range"
              min={MIN_ROW_MULTIPLIER}
              max={MAX_ROW_MULTIPLIER}
              step={1}
              value={rowSizeMultiplier}
              onChange={(e) => setRowSizeMultiplier(clamp(Number(e.target.value), MIN_ROW_MULTIPLIER, MAX_ROW_MULTIPLIER))}
            />
            <span>{rowSizeMultiplier}x</span>
          </div>
          <div className="heatmap-toolbar-item heatmap-row-size-item">
            <span>Heatmap Range</span>
            <input
              type="range"
              min={MIN_HEATMAP_COVERAGE_SCALE}
              max={MAX_HEATMAP_COVERAGE_SCALE}
              step={0.01}
              value={heatmapCoverageScale}
              onChange={(e) => {
                const nextCoverage = clamp(
                  Number(e.target.value),
                  MIN_HEATMAP_COVERAGE_SCALE,
                  MAX_HEATMAP_COVERAGE_SCALE
                );
                setHeatmapCoverageScale(nextCoverage);
              }}
            />
            <span>{heatmapCoverageScale.toFixed(2)}x</span>
          </div>
          <div className="heatmap-timeframes">
            {TIMEFRAME_OPTIONS.map((item) => (
              <button
                key={item}
                className={item === timeframe ? "timeframe-btn active" : "timeframe-btn"}
                type="button"
                onClick={() => setTimeframe(item)}
              >
                {item}
              </button>
            ))}
          </div>
        </div>
        <div className="heatmap-toolbar-right">
          <div className="heatmap-toolbar-item drawing-tool-item">
            <span>Draw</span>
            <select value={drawMode} onChange={(e) => setDrawMode((e.target.value as DrawMode) || "none")}>
              <option value="none">Off</option>
              <option value="hline">Horizontal Line</option>
              <option value="rect">Rectangle</option>
              <option value="ruler">Ruler</option>
            </select>
            <select value={rulerSnapMode} onChange={(e) => setRulerSnapMode((e.target.value as RulerSnapMode) || "off")}>
              <option value="off">Snap: Off</option>
              <option value="close">Snap: Close</option>
              <option value="highlow">Snap: High/Low</option>
            </select>
            {isReplayChart ? (
              <select value={replayOverlayMode} onChange={(e) => setReplayOverlayMode((e.target.value as ReplayOverlayMode) || "both")}>
                <option value="both">Replay: Both</option>
                <option value="trace">Replay: Trace</option>
                <option value="aggregated">Replay: Aggregated</option>
              </select>
            ) : null}
            <button type="button" onClick={() => { setDrawings([]); setRectDraft(null); setRulerDraft(null); setActiveDrawingId(null); }}>
              Clear Draw
            </button>
            <button
              type="button"
              onClick={() =>
                setDrawings((prev) => prev.filter((item) => item.kind !== "ruler"))
              }
            >
              Clear Rulers
            </button>
          </div>
          <button className="heatmap-fullscreen-btn" type="button" onClick={toggleFullscreen}>
            {isFullscreen ? "Exit Full" : "Full Screen"}
          </button>
          <button type="button" onClick={() => { setThresholdLow(0.06); setThresholdHigh(0.88); }}>Reset Thresholds</button>
          <button type="button" onClick={resetHorizontal}>Reset View</button>
          {!isReplayChart ? (
            <button type="button" onClick={() => setShowHeatmap((v) => !v)}>{showHeatmap ? "Hide Heatmap" : "Show Heatmap"}</button>
          ) : null}
        </div>
      </div>
      <div ref={chartWrapRef} className="candles-only-wrap coinglass-chart-wrap">
        <canvas
          ref={canvasRef}
          className="candles-only-canvas coinglass-chart-canvas"
          onPointerDown={onPointerDown}
          onPointerMove={onPointerMove}
          onPointerUp={onPointerUp}
          onPointerCancel={onPointerUp}
          onClick={onCanvasClick}
          onPointerLeave={() => {
            if (!dragRef.current.active && !drawingDragRef.current) {
              setInteraction((prev) => ({
                ...prev,
                crosshair_x: null,
                crosshair_y: null,
                active_bar_index: null,
                active_price: null,
              }));
              if (lockedReplayCandleTs === null) setHoverReplayCandleTs(null);
              setFullscreenProbe(null);
              onLiquidityProbe?.(null);
            }
          }}
          onDoubleClick={onDoubleClick}
          onContextMenu={onContextMenu}
        />
        {isFullscreen && !isReplayChart && (
          <div className="fullscreen-liquidity-probe">
            <div className="fullscreen-liquidity-title">Liquidity Area</div>
            <table className="fullscreen-liquidity-table">
              <thead>
                <tr>
                  <th>Row</th>
                  <th>Left</th>
                  <th>Mid</th>
                  <th>Right</th>
                  <th>Price</th>
                </tr>
              </thead>
              <tbody>
                {[0, 1, 2].map((rowIndex) => {
                  const rowLabel = rowIndex === 0 ? "Top" : rowIndex === 1 ? "Mid" : "Bot";
                  const rowValues = fullscreenProbe?.values?.[rowIndex] ?? [0, 0, 0];
                  const rowPrice = !fullscreenProbe
                    ? NaN
                    : rowIndex === 0
                      ? fullscreenProbe.center_price + fullscreenProbe.bucket_size
                      : rowIndex === 1
                        ? fullscreenProbe.center_price
                        : fullscreenProbe.center_price - fullscreenProbe.bucket_size;
                  return (
                    <tr key={`probe-row-${rowIndex}`}>
                      <td>{rowLabel}</td>
                      {rowValues.map((value, colIndex) => (
                        <td key={`probe-cell-${rowIndex}-${colIndex}`} className={rowIndex === 1 && colIndex === 1 ? "is-center" : ""}>
                          {formatLiquidity(value)}
                        </td>
                      ))}
                      <td>{formatPrice(rowPrice)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {isFullscreen && isReplayChart && (
          <div className="fullscreen-replay-core-table">
            <div className="fullscreen-replay-core-title">Replay Candle Core</div>
            <table>
              <tbody>
                {replayFullscreenRows.slice(0, 8).map((row) => (
                  <tr key={row.label}>
                    <td>{row.label}</td>
                    <td>{row.value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}
