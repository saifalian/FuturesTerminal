import { useEffect, useMemo, useRef, useState, type MutableRefObject } from "react";
import { createPortal } from "react-dom";
import { TerminalState } from "../app/store";
import { WsClient } from "../app/wsClient";
import TerminalPage from "./TerminalPage";
import {
  HeatmapColumn,
  LadderSnapshot,
  MlTrainingEpochMessage,
  ReplayDecisionFrame,
  ReplayStepFrame,
  RunEpochMetricsSeries,
  SourceCoverageUpdate,
  TerminalWsMessage,
} from "../types/market";

type ReplayPageProps = {
  tabId: string;
  symbol: string;
  replaySessionId: string;
  onReplaySessionChange: (replayId: string) => void;
  activeTerminalMode?: "live" | "replay";
  onTerminalModeChange?: (mode: "live" | "replay") => void;
};

type MlRunSummary = {
  run_id: string;
  pair_symbol: string;
  status: string;
  created_at: string;
  instance_id?: string;
};

type MlInstanceSummary = {
  instance_id: string;
  pair_symbol: string;
};
type MlProfileReplaySettings = {
  replay_candle_limit?: number;
};

type ReplaySourceMode = "prediction_trace" | "market_event_replay";
type ReplayPerformanceMode = "fast" | "deep";
type ReplayParityStats = {
  replay_id?: string;
  source_mode?: string;
  expected_events?: number;
  processed_events?: number;
  frames_total?: number;
  batch_ms?: number;
  window_start_ms?: number | null;
  window_end_ms?: number | null;
  cursor?: number;
};
type ReplayBubbleStatsMap = Record<
  string,
  {
    short_count: number;
    long_count: number;
    hold_count: number;
    avg_predicted_magnitude_pct: number;
  }
>;

const API_ROOT = "http://127.0.0.1:8000";

function friendlyApiError(raw: unknown, fallback: string): string {
  const text = String(raw ?? "").trim();
  const lower = text.toLowerCase();
  if (!text) return fallback;
  if (lower.includes("live streaming service is disabled") || lower.includes("live streaming disabled")) {
    return "Live Streaming is disabled in Service Settings.";
  }
  if (lower.includes("replay") && lower.includes("not found")) {
    return "Replay session expired or not found. Please click Start Replay again.";
  }
  if (lower.includes("run") && lower.includes("not found")) {
    return "Selected run was not found. Please reselect a run and retry.";
  }
  return text;
}

function normalizeReplaySourceMode(value: unknown): ReplaySourceMode {
  return String(value || "").trim().toLowerCase() === "market_event_replay" ? "market_event_replay" : "prediction_trace";
}

function toNum(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function fmt(value: unknown, digits = 4): string {
  return toNum(value).toFixed(digits);
}

function linePath(values: number[], width: number, height: number): string {
  if (!values.length) return "";
  if (values.length === 1) {
    const y = (height * 0.5).toFixed(2);
    return `M0.00,${y} L${width.toFixed(2)},${y}`;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1e-9, max - min);
  return values
    .map((v, i) => {
      const x = (i / Math.max(1, values.length - 1)) * width;
      const y = height - ((v - min) / span) * height;
      return `${i === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

function singlePointY(values: number[], height: number): number | null {
  if (values.length !== 1) return null;
  return height * 0.5;
}

function formatRunDateTime(value: string | undefined): string {
  if (!value) return "n/a";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "n/a";
  return dt.toLocaleString();
}

function actionRatio(
  step: ReplayStepFrame | null,
  action: "LONG" | "SHORT" | "HOLD",
  fallbackTotals?: Record<string, number>,
  fallbackSeen?: Record<string, number>,
): string {
  const seenRaw = Number(step?.action_seen?.[action] ?? 0);
  const totalRaw = Number(step?.action_totals?.[action] ?? 0);
  const seen = seenRaw > 0 ? seenRaw : Number(fallbackSeen?.[action] ?? 0);
  const total = totalRaw > 0 ? totalRaw : Number(fallbackTotals?.[action] ?? 0);
  return `${seen}/${total}`;
}

function isReplayStepFrame(value: unknown): value is ReplayStepFrame {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;
  return (
    typeof obj.replay_id === "string" &&
    typeof obj.status === "string" &&
    typeof obj.cursor === "number" &&
    typeof obj.frames_total === "number"
  );
}

function mapReplayMarkerToPlotTs(
  marker: Record<string, unknown>,
  fullCandles: TerminalState["klineSeries"],
  framesTotal: number,
  preserveEpochTs: boolean = false,
): number | null {
  if (!fullCandles.length) return null;
  const firstTs = Number(fullCandles[0].open_ts_ms ?? 0);
  const lastTs = Number(fullCandles[fullCandles.length - 1].open_ts_ms ?? 0);
  const timeframeMs = 60_000;
  const sampleIdx = Number(marker.sample_index ?? marker.step_index ?? marker.ts_key ?? marker.ts_ms ?? NaN);
  const mapBySample = (): number | null => {
    if (!Number.isFinite(sampleIdx) || sampleIdx < 0) return null;
    const denom = Math.max(1, (framesTotal > 1 ? framesTotal - 1 : fullCandles.length - 1));
    const ratio = Math.max(0, Math.min(1, sampleIdx / denom));
    const mappedIdx = Math.max(0, Math.min(fullCandles.length - 1, Math.floor(ratio * Math.max(0, fullCandles.length - 1))));
    return Number(fullCandles[mappedIdx]?.open_ts_ms ?? null);
  };
  // Prefer explicit event time when available so bubbles/markers align to actual candles.
  const rawTs = Number(marker.ts_ms ?? NaN);
  if (Number.isFinite(rawTs) && rawTs > 946684800000) {
    const bucketTs = Math.floor(rawTs / 60_000) * 60_000;
    if (preserveEpochTs) return bucketTs;
    if (!(firstTs > 0 && lastTs > 0 && (bucketTs < firstTs - timeframeMs || bucketTs > lastTs + timeframeMs))) {
      // Snap to nearest available candle bucket.
      let best = fullCandles[0].open_ts_ms;
      let bestDist = Math.abs(best - bucketTs);
      for (const candle of fullCandles) {
        const d = Math.abs(candle.open_ts_ms - bucketTs);
        if (d < bestDist) {
          bestDist = d;
          best = candle.open_ts_ms;
        }
      }
      return best;
    }
  }
  // Fallback to sequence mapping when timestamp is missing/unusable.
  const bySample = mapBySample();
  if (bySample !== null) return bySample;
  return null;
}

function buildReplayCandlesFromHistory(
  replayHistory: Array<Record<string, unknown>>,
  fallbackCandles: TerminalState["klineSeries"],
): TerminalState["klineSeries"] {
  if (!Array.isArray(replayHistory) || replayHistory.length === 0) return fallbackCandles;
  const fallbackCloses = fallbackCandles
    .map((c) => Number(c.close))
    .filter((v) => Number.isFinite(v) && v > 0);
  const fallbackMid =
    fallbackCloses.length > 0
      ? (() => {
          const sorted = [...fallbackCloses].sort((a, b) => a - b);
          return sorted[Math.floor(sorted.length / 2)] ?? 0;
        })()
      : 0;
  // Drop obviously wrong cross-symbol/outlier prices that blow up replay chart Y scale.
  const minAllowed = fallbackMid > 0 ? fallbackMid * 0.8 : 0;
  const maxAllowed = fallbackMid > 0 ? fallbackMid * 1.2 : Number.POSITIVE_INFINITY;
  const minuteMs = 60_000;
  const byMinute = new Map<number, { open: number; high: number; low: number; close: number }>();
  const sorted = [...replayHistory].sort((a, b) => {
    const aIdx = Number(a.sample_index ?? a.step_index ?? NaN);
    const bIdx = Number(b.sample_index ?? b.step_index ?? NaN);
    if (Number.isFinite(aIdx) && Number.isFinite(bIdx) && aIdx !== bIdx) return aIdx - bIdx;
    const aTs = Number(a.plot_ts_ms ?? a.ts_ms ?? NaN);
    const bTs = Number(b.plot_ts_ms ?? b.ts_ms ?? NaN);
    if (Number.isFinite(aTs) && Number.isFinite(bTs) && aTs !== bTs) return aTs - bTs;
    return 0;
  });

  for (const row of sorted) {
    const tsRaw = Number(row.plot_ts_ms ?? row.ts_ms ?? NaN);
    if (!Number.isFinite(tsRaw) || tsRaw <= 946684800000) continue;
    const minuteTs = Math.floor(tsRaw / minuteMs) * minuteMs;
    const price = Number(
      row.price_at_prediction ??
        row.last_trade_price ??
        row.mark_price ??
        row.price ??
        row.close ??
        NaN,
    );
    if (!Number.isFinite(price) || price <= 0) continue;
    if (price < minAllowed || price > maxAllowed) continue;
    const prev = byMinute.get(minuteTs);
    if (!prev) {
      byMinute.set(minuteTs, { open: price, high: price, low: price, close: price });
      continue;
    }
    prev.high = Math.max(prev.high, price);
    prev.low = Math.min(prev.low, price);
    prev.close = price;
  }

  if (byMinute.size === 0) return fallbackCandles;
  const keys = Array.from(byMinute.keys()).sort((a, b) => a - b);
  const out: TerminalState["klineSeries"] = [];
  let prevClose = byMinute.get(keys[0])?.close ?? 0;
  let prevTs = keys[0] - minuteMs;
  for (const ts of keys) {
    for (let gapTs = prevTs + minuteMs; gapTs < ts; gapTs += minuteMs) {
      out.push({
        open_ts_ms: gapTs,
        close_ts_ms: gapTs + 59_999,
        open: prevClose,
        high: prevClose,
        low: prevClose,
        close: prevClose,
        interval: "1m",
        is_closed: true,
      });
    }
    const row = byMinute.get(ts)!;
    out.push({
      open_ts_ms: ts,
      close_ts_ms: ts + 59_999,
      open: row.open,
      high: row.high,
      low: row.low,
      close: row.close,
      interval: "1m",
      is_closed: true,
    });
    prevClose = row.close;
    prevTs = ts;
  }
  return out;
}

function normalizeReplayAction(value: unknown): "LONG" | "SHORT" | "HOLD" {
  const raw = String(value ?? "").trim().toUpperCase();
  if (raw === "LONG" || raw === "UP" || raw === "BUY") return "LONG";
  if (raw === "SHORT" || raw === "DOWN" || raw === "SELL") return "SHORT";
  return "HOLD";
}

export default function ReplayPage({
  tabId,
  symbol,
  replaySessionId,
  onReplaySessionChange,
  activeTerminalMode = "replay",
  onTerminalModeChange,
}: ReplayPageProps) {
  const [runs, setRuns] = useState<MlRunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string>("");
  const [replayStep, setReplayStep] = useState<ReplayStepFrame | null>(null);
  const [replayStatus, setReplayStatus] = useState<"paused" | "playing">("paused");
  const [metrics, setMetrics] = useState<RunEpochMetricsSeries | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [fullChartMode, setFullChartMode] = useState<boolean>(false);
  const [replayTerminalState, setReplayTerminalState] = useState<TerminalState>(
    buildReplayTerminalState(symbol, null, null, [], null, {}, false),
  );
  const [liveBatchText, setLiveBatchText] = useState<string>("");
  const [selectedRunInstanceId, setSelectedRunInstanceId] = useState<string>("");
  const [selectedEpoch, setSelectedEpoch] = useState<number>(0);
  const [selectedSplit, setSelectedSplit] = useState<"train" | "val" | "test">("test");
  const [replaySourceMode, setReplaySourceMode] = useState<ReplaySourceMode>("prediction_trace");
  const [replaySpeed, setReplaySpeed] = useState<number>(1);
  const [replayParityStats, setReplayParityStats] = useState<ReplayParityStats | null>(null);
  const [replayCandleLimit, setReplayCandleLimit] = useState<number>(240);
  const [replayBaseCandles, setReplayBaseCandles] = useState<TerminalState["klineSeries"]>([]);
  const [replayHistoryFrames, setReplayHistoryFrames] = useState<ReplayDecisionFrame[]>([]);
  const [replayGlobalTraceRows, setReplayGlobalTraceRows] = useState<ReplayDecisionFrame[]>([]);
  const [replayGlobalCandleBubbles, setReplayGlobalCandleBubbles] = useState<ReplayBubbleStatsMap>({});
  const [fullscreenActive, setFullscreenActive] = useState<boolean>(false);
  const [fullscreenHost, setFullscreenHost] = useState<Element | null>(null);
  const [timelineCursor, setTimelineCursor] = useState<number>(0);
  const [traceViewerRows, setTraceViewerRows] = useState<ReplayDecisionFrame[]>([]);
  const [traceViewerLoading, setTraceViewerLoading] = useState<boolean>(false);
  const [traceViewerError, setTraceViewerError] = useState<string>("");
  const [traceViewerPage, setTraceViewerPage] = useState<number>(0);
  const [traceExportFormat, setTraceExportFormat] = useState<"txt" | "excel">("excel");
  const [summaryExportFormat, setSummaryExportFormat] = useState<"txt" | "excel">("excel");
  const [summaryExportScope, setSummaryExportScope] = useState<"selected_split" | "all_splits">("selected_split");
  const [traceExportScope, setTraceExportScope] = useState<"full" | "current_page" | "range">("full");
  const [traceExportFromPage, setTraceExportFromPage] = useState<number>(1);
  const [traceExportToPage, setTraceExportToPage] = useState<number>(1);
  const [traceExportIncludeSummary, setTraceExportIncludeSummary] = useState<boolean>(false);
  const [replayPerfMode, setReplayPerfMode] = useState<ReplayPerformanceMode>("fast");
  const TRACE_VIEWER_PAGE_SIZE = 200;
  const tickRef = useRef<number | null>(null);
  const seekTimerRef = useRef<number | null>(null);
  const replayHistoryCacheRef = useRef<Map<string, ReplayDecisionFrame[]>>(new Map());
  const lastNonEmptyReplayHistoryRef = useRef<ReplayDecisionFrame[]>([]);

  const selectedFrame = replayStep?.frame ?? null;
  const epochs = metrics?.epochs ?? [];
  const capturedEpochs = useMemo(
    () =>
      (
        Array.isArray(metrics?.captured_epochs_by_split?.[selectedSplit])
          ? metrics?.captured_epochs_by_split?.[selectedSplit]
          : Array.isArray(metrics?.captured_epochs)
            ? metrics?.captured_epochs
            : []
      )
        .map((v) => Number(v))
        .filter((v) => Number.isFinite(v) && v > 0),
    [metrics, selectedSplit],
  );
  const epochSummaries = useMemo(
    () => (Array.isArray(metrics?.epoch_summaries) ? metrics?.epoch_summaries : []),
    [metrics],
  );
  const splitCaptureEnabled = useMemo(() => {
    const bySplit = (metrics?.captured_epochs_by_split ?? {}) as Record<string, unknown>;
    const splitRows = bySplit?.[selectedSplit];
    if (Array.isArray(splitRows)) return splitRows.length > 0;
    const fallback = Array.isArray(metrics?.captured_epochs) ? metrics?.captured_epochs.length > 0 : false;
    return fallback;
  }, [metrics, selectedSplit]);
  const epochRecordedForSelectedSplit = useMemo(() => {
    // Final trace uses the selected split source, so it still requires split capture.
    if (selectedEpoch <= 0) return splitCaptureEnabled;
    return capturedEpochs.includes(selectedEpoch);
  }, [selectedEpoch, capturedEpochs, splitCaptureEnabled]);
  const notRecordedMessage = useMemo(() => {
    if (splitCaptureEnabled && epochRecordedForSelectedSplit) return "";
    if (!splitCaptureEnabled) {
      return `Replay not available: ${selectedSplit.toUpperCase()} split was not recorded for this run.`;
    }
    return `Replay not available: Epoch ${selectedEpoch} was not recorded for ${selectedSplit.toUpperCase()} split.`;
  }, [splitCaptureEnabled, epochRecordedForSelectedSplit, selectedSplit, selectedEpoch]);
  const replayTraceRequired = replaySourceMode !== "market_event_replay";
  const selectedEpochSummary = useMemo(() => {
    const target = Number(selectedEpoch || 0);
    return (
      epochSummaries.find((item) => {
        const row = item as Record<string, unknown>;
        return Number(row.epoch ?? -1) === target && String(row.split ?? "test") === selectedSplit;
      }) ??
      null
    );
  }, [epochSummaries, selectedEpoch, selectedSplit]);
  const epochActionTotals = useMemo(() => {
    const counts = ((selectedEpochSummary as Record<string, unknown> | null)?.action_counts as Record<string, unknown>) ?? {};
    return {
      LONG: Number(counts.LONG ?? 0),
      SHORT: Number(counts.SHORT ?? 0),
      HOLD: Number(counts.HOLD ?? 0),
    };
  }, [selectedEpochSummary]);
  const replaySeenFromHistory = useMemo(() => {
    const layer = ((replayTerminalState.snapshot as unknown as { decision_layer?: Record<string, unknown> } | null)?.decision_layer ?? {}) as Record<string, unknown>;
    const historyRaw = Array.isArray(layer.replay_history) ? (layer.replay_history as unknown[]) : [];
    const history = historyRaw.filter((item): item is Record<string, unknown> => !!item && typeof item === "object");
    const out = { LONG: 0, SHORT: 0, HOLD: 0 };
    for (const row of history) {
      const action = String(row.action ?? "HOLD").toUpperCase();
      if (action === "LONG") out.LONG += 1;
      else if (action === "SHORT") out.SHORT += 1;
      else out.HOLD += 1;
    }
    return out;
  }, [replayTerminalState.snapshot]);
  const replayLoadedSpanText = useMemo(() => {
    if (!fullChartMode) return "";
    const candles = Array.isArray(replayTerminalState.klineSeries) ? replayTerminalState.klineSeries : [];
    if (candles.length === 0) return "no candles loaded yet";
    if (replayGlobalTraceRows.length === 0) {
      return `${candles.length.toLocaleString()} candles (fallback only: no trace rows loaded)`;
    }
    const firstTs = Number(candles[0]?.open_ts_ms ?? NaN);
    const lastTs = Number(candles[candles.length - 1]?.open_ts_ms ?? NaN);
    if (!Number.isFinite(firstTs) || !Number.isFinite(lastTs) || lastTs < firstTs) {
      return `${candles.length.toLocaleString()} candles`;
    }
    const minutes = Math.max(1, Math.round((lastTs - firstTs) / 60_000) + 1);
    const hours = minutes / 60;
    return `${candles.length.toLocaleString()} candles (~${hours.toFixed(1)}h)`;
  }, [fullChartMode, replayTerminalState.klineSeries, replayGlobalTraceRows.length]);
  const latestEpoch = epochs.length > 0 ? epochs[epochs.length - 1] : null;

  const globalTraceLoadKeyRef = useRef<string>("");

  useEffect(() => {
    if (!capturedEpochs.length) {
      setSelectedEpoch(0);
      return;
    }
    if (!capturedEpochs.includes(selectedEpoch)) {
      setSelectedEpoch(capturedEpochs[0]);
    }
  }, [capturedEpochs, selectedEpoch]);

  useEffect(() => {
    setReplayTerminalState(buildReplayTerminalState(symbol, null, null, [], null, {}, fullChartMode));
  }, [symbol, fullChartMode]);

  useEffect(() => {
    const ws = new WsClient(tabId);
    ws.connect((message: TerminalWsMessage) => {
      if (message.type === "ml_training_epoch") {
        const payload = message as MlTrainingEpochMessage;
        if (!selectedRunId || payload.run_id !== selectedRunId) return;
        setMetrics((prev) => {
          if (!prev) return prev;
          const incoming = payload.payload ?? {};
          const nextEpoch = Number(incoming.epoch || 0);
          const existing = prev.epochs.filter((row) => Number(row.epoch || 0) !== nextEpoch);
          return { ...prev, epochs: [...existing, incoming].sort((a, b) => Number(a.epoch || 0) - Number(b.epoch || 0)) };
        });
        return;
      }
      if (message.type === "ml_training_batch") {
        const payload = message as unknown as { run_id?: string; payload?: Record<string, unknown> };
        if (!selectedRunId || payload.run_id !== selectedRunId) return;
        const p = payload.payload ?? {};
        setLiveBatchText(
          `epoch ${toNum(p.epoch, 0)}/${toNum(p.epochs_total, 0)} batch ${toNum(p.batch, 0)}/${toNum(p.batches_total, 0)} loss=${fmt(p.train_loss, 5)}`
        );
      }
    });
    return () => ws.close();
  }, [tabId, selectedRunId]);

  useEffect(() => {
    let cancelled = false;
    async function loadRuns() {
      try {
        const query = new URLSearchParams({ pair_symbol: symbol });
        const res = await fetch(`${API_ROOT}/ml/runs?${query.toString()}`);
        const data = (await res.json()) as { runs?: MlRunSummary[] };
        if (cancelled) return;
        const list = Array.isArray(data.runs) ? data.runs : [];
        setRuns(list);
        if (!selectedRunId && list.length > 0) setSelectedRunId(list[0].run_id);
      } catch {
        if (!cancelled) setRuns([]);
      }
    }
    void loadRuns();
    return () => {
      cancelled = true;
    };
  }, [symbol, selectedRunId]);

  useEffect(() => {
    if (!selectedRunId) return;
    let cancelled = false;
    async function loadRunMeta() {
      try {
        const res = await fetch(`${API_ROOT}/ml/runs/${selectedRunId}`);
        const data = (await res.json()) as { instance_id?: string };
        if (cancelled) return;
        const resolved = String(data.instance_id || "").trim();
        if (resolved) {
          setSelectedRunInstanceId(resolved);
          return;
        }
        // Fallback for older runs without instance_id stored: resolve by pair symbol.
        const instancesRes = await fetch(`${API_ROOT}/ml/instances`);
        const instancesData = (await instancesRes.json()) as { instances?: MlInstanceSummary[] };
        if (cancelled) return;
        const list = Array.isArray(instancesData.instances) ? instancesData.instances : [];
        const match = list.find((item) => String(item.pair_symbol || "").toUpperCase() === symbol.toUpperCase());
        setSelectedRunInstanceId(String(match?.instance_id || ""));
      } catch {
        if (!cancelled) setSelectedRunInstanceId("");
      }
    }
    void loadRunMeta();
    return () => {
      cancelled = true;
    };
  }, [selectedRunId]);

  useEffect(() => {
    const updateFullscreenState = () => {
      const doc = document as Document & {
        webkitFullscreenElement?: Element | null;
        mozFullScreenElement?: Element | null;
        msFullscreenElement?: Element | null;
      };
      const host =
        doc.fullscreenElement ??
        doc.webkitFullscreenElement ??
        doc.mozFullScreenElement ??
        doc.msFullscreenElement ??
        null;
      const active = !!host;
      setFullscreenActive(active);
      setFullscreenHost(host);
    };
    updateFullscreenState();
    document.addEventListener("fullscreenchange", updateFullscreenState);
    document.addEventListener("webkitfullscreenchange", updateFullscreenState as EventListener);
    document.addEventListener("mozfullscreenchange", updateFullscreenState as EventListener);
    document.addEventListener("MSFullscreenChange", updateFullscreenState as EventListener);
    return () => {
      document.removeEventListener("fullscreenchange", updateFullscreenState);
      document.removeEventListener("webkitfullscreenchange", updateFullscreenState as EventListener);
      document.removeEventListener("mozfullscreenchange", updateFullscreenState as EventListener);
      document.removeEventListener("MSFullscreenChange", updateFullscreenState as EventListener);
    };
  }, []);

  useEffect(() => {
    if (!selectedRunInstanceId) return;
    let cancelled = false;
    async function loadReplaySettings() {
      try {
        const res = await fetch(`${API_ROOT}/ml/instances/${selectedRunInstanceId}/profile`);
        const data = (await res.json()) as MlProfileReplaySettings;
        if (cancelled) return;
        const nextLimit = Number(data?.replay_candle_limit ?? 240);
        setReplayCandleLimit(Number.isFinite(nextLimit) ? Math.max(30, Math.min(5000, Math.round(nextLimit))) : 240);
      } catch {
        if (!cancelled) setReplayCandleLimit(240);
      }
    }
    void loadReplaySettings();
    return () => {
      cancelled = true;
    };
  }, [selectedRunInstanceId]);

  useEffect(() => {
    let cancelled = false;
    async function loadBaseCandles() {
      try {
        let rows: Array<{ ts_ms: number; open: number; high: number; low: number; close: number }> = [];
        if (selectedRunInstanceId) {
          const res = await fetch(
            `${API_ROOT}/ml/instances/${selectedRunInstanceId}/bot/trade-map-candles?limit=${encodeURIComponent(String(replayCandleLimit))}&for_replay=1`,
          );
          const data = (await res.json()) as {
            candles?: Array<{ ts_ms: number; open: number; high: number; low: number; close: number }>;
          };
          rows = Array.isArray(data.candles) ? data.candles : [];
        }
        if (!rows.length) {
          const fallbackRes = await fetch(
            `${API_ROOT}/ml/replay/candles?pair_symbol=${encodeURIComponent(symbol)}&limit=${encodeURIComponent(String(replayCandleLimit))}`,
          );
          const fallbackData = (await fallbackRes.json()) as {
            candles?: Array<{ ts_ms: number; open: number; high: number; low: number; close: number }>;
          };
          rows = Array.isArray(fallbackData.candles) ? fallbackData.candles : [];
        }
        if (cancelled) return;
        const mapped = rows
          .filter((c) => Number.isFinite(Number(c.ts_ms)))
          .map((c) => ({
            open_ts_ms: Number(c.ts_ms),
            close_ts_ms: Number(c.ts_ms) + 59_999,
            open: Number(c.open),
            high: Number(c.high),
            low: Number(c.low),
            close: Number(c.close),
            interval: "1m",
            is_closed: true,
          }));
        setReplayBaseCandles(mapped);
      } catch {
        if (!cancelled) setReplayBaseCandles([]);
      }
    }
    void loadBaseCandles();
    return () => {
      cancelled = true;
    };
  }, [selectedRunInstanceId, symbol, replayCandleLimit]);

  useEffect(() => {
    if (!selectedRunId) return;
    let cancelled = false;
    async function loadMetrics() {
      try {
        const res = await fetch(`${API_ROOT}/ml/runs/${selectedRunId}/metrics`);
        const data = (await res.json()) as RunEpochMetricsSeries;
        if (!cancelled) setMetrics(data);
      } catch {
        if (!cancelled) setMetrics(null);
      }
    }
    void loadMetrics();
    const timer = window.setInterval(() => {
      void loadMetrics();
    }, 3000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [selectedRunId]);

  useEffect(() => {
    if (!replaySessionId) {
      setReplayStep(null);
      setReplayStatus("paused");
      setReplaySourceMode("prediction_trace");
      setReplayParityStats(null);
      setReplayHistoryFrames([]);
      setReplayGlobalTraceRows([]);
      lastNonEmptyReplayHistoryRef.current = [];
      replayHistoryCacheRef.current.clear();
      setReplayTerminalState(buildReplayTerminalState(symbol, null, null, replayBaseCandles, null, replayGlobalCandleBubbles, fullChartMode));
      return;
    }
    let cancelled = false;
    async function loadStep() {
      try {
        const res = await fetch(`${API_ROOT}/ml/replay/${replaySessionId}/step`);
        const data = (await res.json()) as ReplayStepFrame | { detail?: string };
      if (!isReplayStepFrame(data)) {
        if (!cancelled) {
          setReplayStep(null);
          setReplayStatus("paused");
          setReplayParityStats(null);
          setReplayHistoryFrames([]);
            const detail = typeof (data as { detail?: string }).detail === "string" ? (data as { detail?: string }).detail! : "";
            setError(friendlyApiError(detail, "Replay step unavailable."));
            if (typeof (data as { detail?: string }).detail === "string" && ((data as { detail?: string }).detail || "").toLowerCase().includes("not found")) {
              onReplaySessionChange("");
            }
          }
          return;
        }
        if (cancelled) return;
        setError("");
        setReplayStep(data);
        setReplayStatus((data.status as "paused" | "playing") || "paused");
        const nextMode = normalizeReplaySourceMode(data.source_mode);
        setReplaySourceMode(nextMode);
        const eventHistory = nextMode === "market_event_replay"
          ? []
          : await loadReplayHistoryCached(
              replayHistoryCacheRef,
              replaySessionId,
              Number(data.cursor ?? 0),
              Number(data.frames_total ?? 0),
              replayCandleLimit,
              String(data.split ?? selectedSplit),
              Number(data.epoch_index ?? selectedEpoch),
            );
        if (!cancelled) {
          const effectiveHistory = eventHistory.length > 0 ? eventHistory : lastNonEmptyReplayHistoryRef.current;
          const historyForState =
            fullChartMode && replayGlobalTraceRows.length > 0
              ? replayGlobalTraceRows
              : effectiveHistory;
          if (effectiveHistory.length > 0) {
            lastNonEmptyReplayHistoryRef.current = effectiveHistory;
          }
          setReplayHistoryFrames(effectiveHistory);
          setTimelineCursor(Math.max(0, Number(data.cursor ?? 0)));
          setReplayTerminalState((prev) =>
            buildReplayTerminalState(symbol, data, prev, replayBaseCandles, historyForState, replayGlobalCandleBubbles, fullChartMode),
          );
          if (nextMode === "market_event_replay") {
            void fetchReplayParityStats(replaySessionId);
          } else {
            setReplayParityStats(null);
          }
        }
      } catch {
        if (!cancelled) {
          setReplayStep(null);
          setReplayStatus("paused");
          setReplayParityStats(null);
          setError("Failed to load replay step.");
        }
      }
    }
    void loadStep();
    return () => {
      cancelled = true;
    };
  }, [replaySessionId, symbol, replayBaseCandles, replayGlobalCandleBubbles, replayGlobalTraceRows, fullChartMode]);

  useEffect(() => {
    if (replayStatus !== "playing" || !replaySessionId) return;
    const tickMs =
      replaySpeed >= 6 ? 450 :
      replaySpeed >= 4 ? 700 :
      replaySpeed >= 2 ? 1200 :
      3000; // Normal: deliberately slow/readable
    tickRef.current = window.setInterval(async () => {
      try {
        const res = await fetch(`${API_ROOT}/ml/replay/${replaySessionId}/control`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "play", speed: replaySpeed, source_mode: replaySourceMode }),
        });
        const control = (await res.json()) as { status?: string };
        const stepRes = await fetch(`${API_ROOT}/ml/replay/${replaySessionId}/step`);
        const stepData = (await stepRes.json()) as ReplayStepFrame;
        if (!isReplayStepFrame(stepData)) {
          setReplayStatus("paused");
          setError("Replay session ended or unavailable.");
          return;
        }
        setReplayStep(stepData);
        const nextMode = normalizeReplaySourceMode(stepData.source_mode);
        setReplaySourceMode(nextMode);
        setTimelineCursor(Math.max(0, Number(stepData.cursor ?? 0)));
        setError("");
        const effectiveHistory = replayHistoryFrames.length > 0 ? replayHistoryFrames : lastNonEmptyReplayHistoryRef.current;
        const historyForState =
          fullChartMode && replayGlobalTraceRows.length > 0
            ? replayGlobalTraceRows
            : effectiveHistory;
        setReplayTerminalState((prev) =>
          buildReplayTerminalState(
            symbol,
            stepData,
            prev,
            replayBaseCandles,
            historyForState,
            replayGlobalCandleBubbles,
            fullChartMode,
          ),
        );
        if (nextMode === "market_event_replay") {
          void fetchReplayParityStats(replaySessionId);
        } else {
          setReplayParityStats(null);
        }
        setReplayStatus((control.status as "paused" | "playing") || (stepData.status as "paused" | "playing") || "paused");
      } catch {
        setReplayStatus("paused");
        setError("Replay playback interrupted.");
      }
    }, tickMs);
    return () => {
      if (tickRef.current) {
        window.clearInterval(tickRef.current);
        tickRef.current = null;
      }
    };
  }, [replayStatus, replaySessionId, replaySourceMode, replaySpeed, symbol, replayBaseCandles, replayHistoryFrames, replayGlobalTraceRows, replayGlobalCandleBubbles, fullChartMode]);

  useEffect(() => {
    if (!replayStep) return;
    setTimelineCursor(Math.max(0, Number(replayStep.cursor ?? 0)));
  }, [replayStep?.cursor]);

  useEffect(() => {
    if (!replaySessionId || replaySourceMode !== "market_event_replay") {
      if (replaySourceMode !== "market_event_replay") setReplayParityStats(null);
      return;
    }
    let cancelled = false;
    const tick = async () => {
      if (cancelled) return;
      await fetchReplayParityStats(replaySessionId);
    };
    void tick();
    const timer = window.setInterval(() => {
      void tick();
    }, 1200);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [replaySessionId, replaySourceMode]);

  useEffect(() => {
    let cancelled = false;
    async function loadGlobalBubbleCounts() {
      if (replaySourceMode !== "prediction_trace") {
        setReplayGlobalTraceRows([]);
        setReplayGlobalCandleBubbles({});
        return;
      }
      if (!selectedRunId) {
        setReplayGlobalTraceRows([]);
        setReplayGlobalCandleBubbles({});
        return;
      }
      // Keep last non-empty global map during transient candle reloads.
      if (replayBaseCandles.length === 0) {
        return;
      }
      const loadKey = JSON.stringify({
        run: selectedRunId,
        mode: replaySourceMode,
        candles: replayBaseCandles.length,
        selectedSplit,
        selectedEpoch,
      });
      if (globalTraceLoadKeyRef.current === loadKey) return;
      globalTraceLoadKeyRef.current = loadKey;
      const selectedEpochSafe = Math.max(0, Math.floor(Number(selectedEpoch ?? 0)));
      // On-demand, bounded fetch only for selected split+epoch.
      // This avoids epoch fan-out and huge response payloads during active runs.
      const globalLimit = replayPerfMode === "deep" ? 250000 : 120000;
      const traces = await Promise.all([fetchRunPredictionTraceRows(selectedRunId, selectedSplit, selectedEpochSafe, globalLimit)]);
      if (cancelled) return;
      const allRows = traces.flat();
      if (allRows.length === 0) {
        setReplayGlobalTraceRows([]);
        setReplayGlobalCandleBubbles({});
        return;
      }
      setReplayGlobalTraceRows(allRows);
      const combined = buildReplayCandleBubbleStats(
        allRows as Array<Record<string, unknown>>,
        replayBaseCandles,
        Math.max(1, allRows.length),
      );
      if (Object.keys(combined).length > 0) {
        setReplayGlobalCandleBubbles(combined);
      } else {
        setReplayGlobalCandleBubbles({});
      }
    }
    void loadGlobalBubbleCounts();
    return () => {
      cancelled = true;
    };
  }, [selectedRunId, replaySourceMode, replayBaseCandles.length, selectedSplit, selectedEpoch, replayPerfMode]);
  
  useEffect(() => {
    if (!fullChartMode || replaySourceMode !== "prediction_trace") return;
    if (replayGlobalTraceRows.length > 0) return;
    const noFrames = Number(replayStep?.frames_total ?? 0) <= 0;
    if (!noFrames) return;
    setError("No prediction-trace rows found for this run/split/epoch. Choose a recorded split/epoch.");
  }, [fullChartMode, replaySourceMode, replayGlobalTraceRows.length, replayStep?.frames_total]);

  useEffect(() => {
    return () => {
      if (seekTimerRef.current) {
        window.clearTimeout(seekTimerRef.current);
        seekTimerRef.current = null;
      }
    };
  }, []);

  const lossPath = useMemo(() => linePath(epochs.map((e) => toNum(e.val_loss)), 420, 90), [epochs]);
  const f1Path = useMemo(() => linePath(epochs.map((e) => toNum(e.val_macro_f1)), 420, 90), [epochs]);
  const accPath = useMemo(() => linePath(epochs.map((e) => toNum(e.direction_accuracy)), 420, 90), [epochs]);
  const classF1DownPath = useMemo(() => linePath(epochs.map((e) => toNum(e.direction_f1_down)), 420, 90), [epochs]);
  const classF1FlatPath = useMemo(() => linePath(epochs.map((e) => toNum(e.direction_f1_flat)), 420, 90), [epochs]);
  const classF1UpPath = useMemo(() => linePath(epochs.map((e) => toNum(e.direction_f1_up)), 420, 90), [epochs]);
  const lossSingleY = useMemo(() => singlePointY(epochs.map((e) => toNum(e.val_loss)), 90), [epochs]);
  const f1SingleY = useMemo(() => singlePointY(epochs.map((e) => toNum(e.val_macro_f1)), 90), [epochs]);
  const accSingleY = useMemo(() => singlePointY(epochs.map((e) => toNum(e.direction_accuracy)), 90), [epochs]);
  const classF1DownSingleY = useMemo(() => singlePointY(epochs.map((e) => toNum(e.direction_f1_down)), 90), [epochs]);
  const classF1FlatSingleY = useMemo(() => singlePointY(epochs.map((e) => toNum(e.direction_f1_flat)), 90), [epochs]);
  const classF1UpSingleY = useMemo(() => singlePointY(epochs.map((e) => toNum(e.direction_f1_up)), 90), [epochs]);
  const confusionMatrix = useMemo(() => {
    const matrix = metrics?.final_metrics?.direction_confusion_matrix;
    if (!Array.isArray(matrix)) return null;
    return matrix as number[][];
  }, [metrics]);
  const finalMetrics = useMemo(() => {
    const fm = (metrics?.final_metrics ?? {}) as Record<string, unknown>;
    const num = (key: string): number => {
      const raw = Number(fm[key] ?? NaN);
      return Number.isFinite(raw) ? raw : 0;
    };
    return {
      samplesTest: Math.max(0, Math.round(num("samples_test"))),
      directionAccuracy: num("direction_accuracy"),
      macroF1: num("direction_f1_macro"),
      magnitudeRmse: num("magnitude_rmse"),
      pnlProxy: num("pnl_proxy"),
      sharpeProxy: num("sharpe_proxy"),
      qualityAccuracy: num("quality_accuracy"),
      bestValLoss: num("best_val_loss"),
    };
  }, [metrics]);
  const tracePriceDecimals = useMemo(() => {
    if (!traceViewerRows.length) return 4;
    const sample = traceViewerRows
      .map((r) => Number(r.price_at_prediction ?? NaN))
      .find((v) => Number.isFinite(v) && v > 0);
    const sampleNum = Number(sample ?? NaN);
    if (!Number.isFinite(sampleNum)) return 4;
    if (sampleNum >= 1000) return 2;
    if (sampleNum >= 100) return 3;
    if (sampleNum >= 1) return 4;
    return 6;
  }, [traceViewerRows]);
  const traceActionCountsByPrice = useMemo(() => {
    const out = new Map<string, { l: number; s: number; h: number }>();
    const normalizeAction = (value: unknown): "LONG" | "SHORT" | "HOLD" => {
      const raw = String(value ?? "").trim().toUpperCase();
      if (raw === "LONG" || raw === "UP" || raw === "BUY") return "LONG";
      if (raw === "SHORT" || raw === "DOWN" || raw === "SELL") return "SHORT";
      return "HOLD";
    };
    for (const row of traceViewerRows) {
      const price = Number(row.price_at_prediction ?? NaN);
      if (!Number.isFinite(price) || price <= 0) continue;
      const key = price.toFixed(tracePriceDecimals);
      const bucket = out.get(key) ?? { l: 0, s: 0, h: 0 };
      const action = normalizeAction(row.predicted_label ?? row.action);
      if (action === "LONG") bucket.l += 1;
      else if (action === "SHORT") bucket.s += 1;
      else bucket.h += 1;
      out.set(key, bucket);
    }
    return out;
  }, [traceViewerRows, tracePriceDecimals]);

  async function fetchReplayParityStats(targetReplayId: string) {
    try {
      const res = await fetch(`${API_ROOT}/ml/replay/${encodeURIComponent(targetReplayId)}/parity-stats`, { cache: "no-store" });
      if (!res.ok) return;
      const data = (await res.json()) as ReplayParityStats;
      setReplayParityStats(data);
    } catch {
      // keep previous stats on transient errors
    }
  }

  async function fetchRunPredictionTraceRows(
    runId: string,
    split: "train" | "val" | "test",
    epochIndex: number,
    limit: number = 250000,
  ): Promise<ReplayDecisionFrame[]> {
    try {
      const res = await fetch(
        `${API_ROOT}/ml/runs/${encodeURIComponent(runId)}/prediction-trace?split=${encodeURIComponent(split)}&epoch_index=${Math.max(0, Math.floor(epochIndex))}&limit=${Math.max(1000, Math.min(250000, Math.floor(limit)))}`,
        { cache: "no-store" },
      );
      if (!res.ok) return [];
      const data = (await res.json()) as { rows?: unknown[] };
      if (!Array.isArray(data.rows)) return [];
      return data.rows.filter((row): row is ReplayDecisionFrame => !!row && typeof row === "object");
    } catch {
      return [];
    }
  }

  async function loadTraceViewerRows() {
    if (!selectedRunId) return;
    setTraceViewerLoading(true);
    setTraceViewerError("");
    try {
      const loadLimit = replayPerfMode === "deep" ? 250000 : 120000;
      const rows = await fetchRunPredictionTraceRows(selectedRunId, selectedSplit, selectedEpoch, loadLimit);
      setTraceViewerRows(rows);
      setTraceViewerPage(0);
    } catch {
      setTraceViewerError("Could not load full trace rows.");
    } finally {
      setTraceViewerLoading(false);
    }
  }

  function downloadBlob(filename: string, mime: string, content: string) {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  function exportEpochSummaries() {
    const allRows = Array.isArray(epochSummaries) ? epochSummaries : [];
    const rows = allRows
      .filter((row) => {
        if (summaryExportScope === "all_splits") return true;
        return String((row as Record<string, unknown>).split ?? "test") === selectedSplit;
      })
      .sort((a, b) => {
        const sa = String((a as Record<string, unknown>).split ?? "");
        const sb = String((b as Record<string, unknown>).split ?? "");
        if (sa !== sb) return sa.localeCompare(sb);
        return Number((a as Record<string, unknown>).epoch ?? 0) - Number((b as Record<string, unknown>).epoch ?? 0);
      });
    if (!rows.length) {
      setError("No epoch summaries available to export.");
      return;
    }
    const baseName = `epoch_summary_${summaryExportScope === "all_splits" ? "all_splits" : selectedSplit}_run_${selectedRunId || "unknown"}`;
    const formatSummaryTextBlock = (row: Record<string, unknown>): string[] => {
      const lines: string[] = [];
      const split = String(row.split ?? "test");
      const epoch = Number(row.epoch ?? 0);
      const samples = Number(row.samples ?? 0);
      const acc = Number(row.accuracy ?? 0) * 100;
      const ear = Number(row.entry_allowed_rate ?? 0) * 100;
      const blocked = Number(row.blocked_count ?? 0);
      const dpr = Number(row.directional_prediction_rate ?? 0) * 100;
      const dar = Number(row.directional_action_rate ?? 0) * 100;
      const actions = (row.action_counts as Record<string, unknown>) ?? {};
      const predDist = (row.predicted_class_distribution as Record<string, unknown>) ?? {};
      const blockedReasons = (row.blocked_reasons as Record<string, unknown>) ?? {};
      const tr = (row.trade_rate_by_class as Record<string, unknown>) ?? {};
      const edge = (row.expected_edge_by_confidence_bucket as Array<Record<string, unknown>>) ?? [];
      const hist = (row.confidence_distribution_histogram as Array<Record<string, unknown>>) ?? [];
      const spread = (row.confidence_spread_stats as Record<string, unknown>) ?? {};
      const byLabel = (row.future_return_distribution_by_label as Record<string, unknown>) ?? {};
      lines.push(`Split/Epoch\t${split} / ${epoch}`);
      lines.push(`Samples\t${samples.toLocaleString()}`);
      lines.push(`Accuracy\t${acc.toFixed(2)}%`);
      lines.push(`Entry Allowed Rate\t${ear.toFixed(2)}%`);
      lines.push(`Blocked Count\t${blocked.toLocaleString()}`);
      lines.push(`Directional Pred Rate\t${dpr.toFixed(2)}%`);
      lines.push(`Directional Action Rate\t${dar.toFixed(2)}%`);
      lines.push(`Abstention Rate\t${(Number(row.abstention_rate ?? 0) * 100).toFixed(2)}%`);
      lines.push(`Abstention Correctness\t${(Number(row.abstention_correctness ?? 0) * 100).toFixed(2)}%`);
      lines.push(
        `Actions (L/S/H)\t${Number(actions.LONG ?? 0).toLocaleString()} / ${Number(actions.SHORT ?? 0).toLocaleString()} / ${Number(actions.HOLD ?? 0).toLocaleString()}`,
      );
      lines.push(
        `Predicted Class Dist\tUP: ${Number(predDist.up_count ?? 0).toLocaleString()} (${(Number(predDist.up_pct ?? 0) * 100).toFixed(2)}%), FLAT: ${Number(predDist.flat_count ?? 0).toLocaleString()} (${(Number(predDist.flat_pct ?? 0) * 100).toFixed(2)}%), DOWN: ${Number(predDist.down_count ?? 0).toLocaleString()} (${(Number(predDist.down_pct ?? 0) * 100).toFixed(2)}%)`,
      );
      lines.push(
        `Blocked Reasons\t${Object.entries(blockedReasons).map(([k, v]) => `${k}: ${Number(v).toLocaleString()}`).join(", ") || "none"}`,
      );
      lines.push(`Gate Paralyzed\t${Boolean(row.gate_paralyzed) ? "yes" : "no"}`);
      lines.push("Trade Rate by Class");
      lines.push(`LONG when actual UP\t${(Number(tr.long_rate_when_actual_up ?? 0) * 100).toFixed(2)}%`);
      lines.push(`SHORT when actual DOWN\t${(Number(tr.short_rate_when_actual_down ?? 0) * 100).toFixed(2)}%`);
      lines.push(`False LONG rate\t${(Number(tr.false_long_rate ?? 0) * 100).toFixed(2)}%`);
      lines.push(`False SHORT rate\t${(Number(tr.false_short_rate ?? 0) * 100).toFixed(2)}%`);
      lines.push("Expected Edge by Confidence Bucket");
      if (!edge.length) lines.push("none");
      for (const b of edge) {
        lines.push(
          `${String(b.bucket ?? "-")}\tn=${Number(b.sample_count ?? 0)}\tavg%=${fmt(Number(b.avg_future_return_pct ?? 0), 4)}\tmed%=${fmt(Number(b.median_future_return_pct ?? 0), 4)}\thit%=${(Number(b.direction_hit_rate ?? 0) * 100).toFixed(2)}%`,
        );
      }
      lines.push("Confidence Distribution");
      if (!hist.length) lines.push("none");
      for (const h of hist) {
        lines.push(`${String(h.bucket ?? "-")}\t${Number(h.count ?? 0).toLocaleString()} (${(Number(h.pct ?? 0) * 100).toFixed(2)}%)`);
      }
      lines.push("Probability Spread");
      lines.push(`mean top1 prob\t${fmt(Number(spread.mean_top1_prob ?? 0), 4)}`);
      lines.push(`mean top2 prob\t${fmt(Number(spread.mean_top2_prob ?? 0), 4)}`);
      lines.push(`mean confidence gap\t${fmt(Number(spread.mean_confidence_gap ?? 0), 4)}`);
      lines.push("Future Return Distribution by Label");
      for (const key of ["up", "flat", "down"]) {
        const l = ((byLabel as Record<string, unknown>)[key] as Record<string, unknown>) ?? {};
        lines.push(
          `${key.toUpperCase()}\tcount=${Number(l.count ?? 0)}\tmean=${fmt(Number(l.mean ?? 0), 4)}\tmedian=${fmt(Number(l.median ?? 0), 4)}\tp25=${fmt(Number(l.p25 ?? 0), 4)}\tp75=${fmt(Number(l.p75 ?? 0), 4)}\tmin=${fmt(Number(l.min ?? 0), 4)}\tmax=${fmt(Number(l.max ?? 0), 4)}`,
        );
      }
      const fm = (metrics?.final_metrics ?? {}) as Record<string, unknown>;
      const fs = (fm.feature_separability as Record<string, unknown>) ?? {};
      lines.push("Feature Separability");
      if (!Object.keys(fs).length) lines.push("none");
      for (const [name, raw] of Object.entries(fs)) {
        if (!raw || typeof raw !== "object") continue;
        const obj = raw as Record<string, unknown>;
        const by = (obj.by_label as Record<string, unknown>) ?? {};
        const up = (by.up as Record<string, unknown>) ?? {};
        const flat = (by.flat as Record<string, unknown>) ?? {};
        const down = (by.down as Record<string, unknown>) ?? {};
        const sep = (obj.separability as Record<string, unknown>) ?? {};
        lines.push(
          `${name}\tup mean=${fmt(Number(up.mean ?? 0), 4)}\tflat mean=${fmt(Number(flat.mean ?? 0), 4)}\tdown mean=${fmt(Number(down.mean ?? 0), 4)}\tsep U-D=${fmt(Number(sep.up_down ?? 0), 3)}\tsep U-F=${fmt(Number(sep.up_flat ?? 0), 3)}\tsep F-D=${fmt(Number(sep.flat_down ?? 0), 3)}\tcorr(ret)=${fmt(Number(obj.corr_with_future_return ?? 0), 3)}`,
        );
      }
      const spacing = (fm.triple_barrier_step_spacing_stats as Record<string, unknown>) ?? {};
      if (Object.keys(spacing).length) {
        lines.push("Triple-Barrier Time Spacing");
        lines.push(`median_step_delta_ms\t${fmt(Number(spacing.median_step_delta_ms ?? 0), 3)}`);
        lines.push(`min_step_delta_ms\t${fmt(Number(spacing.min_step_delta_ms ?? 0), 3)}`);
        lines.push(`max_step_delta_ms\t${fmt(Number(spacing.max_step_delta_ms ?? 0), 3)}`);
        lines.push(`estimated_timeout_seconds\t${fmt(Number(fm.estimated_timeout_seconds ?? spacing.estimated_timeout_seconds ?? 0), 3)}`);
        lines.push(`non_monotonic_timestamp_count\t${Number(spacing.non_monotonic_timestamp_count ?? 0)}`);
        lines.push(`contiguous_within_tolerance\t${Boolean(spacing.contiguous_within_tolerance) ? "yes" : "no"}`);
      }
      if ("label_price_rejected_rate" in fm || "label_price_quality_guard_triggered" in fm) {
        lines.push("Label-Price Quality Guard");
        lines.push(`source\t${String(fm.label_price_source_used ?? "bookticker_mid_state")}`);
        lines.push(`rejected rate\t${(Number(fm.label_price_rejected_rate ?? 0) * 100).toFixed(4)}%`);
        lines.push(`rejected count\t${Number(fm.label_price_rejected_count ?? 0)}`);
        lines.push(`bookticker updates\t${Number(fm.bookticker_updates_total ?? 0)}`);
        lines.push(`threshold\t${(Number(fm.max_label_price_rejected_rate_used ?? 0.005) * 100).toFixed(4)}%`);
        lines.push(`guard triggered\t${Boolean(fm.label_price_quality_guard_triggered) ? "yes" : "no"}`);
      }
      if ("temperature_value" in fm || "deterministic_replay_verified" in fm) {
        lines.push("Calibration + Determinism");
        lines.push(`temperature_value\t${fmt(Number(fm.temperature_value ?? 1), 6)}`);
        lines.push(`deterministic_replay_verified\t${Boolean(fm.deterministic_replay_verified) ? "yes" : "no"}`);
        lines.push(`decision_trace_hash_match\t${Boolean(fm.decision_trace_hash_match) ? "yes" : "no"}`);
      }
      const confSweep = Array.isArray(fm.confidence_threshold_sweep)
        ? (fm.confidence_threshold_sweep as Array<Record<string, unknown>>)
        : [];
      if (confSweep.length) {
        lines.push("Confidence Threshold Sweep");
        lines.push(
          "threshold\taction_rate\tlong_rate\tshort_rate\thold_rate\tmacro_f1\tf1_directional\tmean_prob_gap\tmean_entropy\tece_score\tbrier_score\tpnl_proxy_after_cost\tsharpe_proxy_after_cost\tvalid\trejection_reasons",
        );
        for (const s of confSweep) {
          lines.push(
            `${fmt(Number(s.threshold ?? 0), 2)}\t${fmt(Number(s.action_rate ?? 0), 4)}\t${fmt(Number(s.long_rate ?? 0), 4)}\t${fmt(Number(s.short_rate ?? 0), 4)}\t${fmt(Number(s.hold_rate ?? 0), 4)}\t${fmt(Number(s.macro_f1 ?? 0), 4)}\t${fmt(Number(s.f1_directional ?? 0), 4)}\t${fmt(Number(s.mean_prob_gap ?? 0), 4)}\t${fmt(Number(s.mean_entropy ?? 0), 4)}\t${fmt(Number(s.ece_score ?? 0), 4)}\t${fmt(Number(s.brier_score ?? 0), 4)}\t${fmt(Number(s.pnl_proxy_after_cost ?? 0), 6)}\t${fmt(Number(s.sharpe_proxy_after_cost ?? 0), 6)}\t${Boolean(s.valid) ? "yes" : "no"}\t${Array.isArray(s.rejection_reasons) ? (s.rejection_reasons as Array<unknown>).map((v) => String(v)).join("|") : ""}`,
          );
        }
        lines.push(`recommended_confidence_threshold\t${fmt(Number(fm.recommended_confidence_threshold ?? 0), 4)}`);
      }
      if ("walk_forward_enabled" in fm || "recommended_live_mode" in fm || "approval_reason" in fm) {
        lines.push("Trade-Outcome Final Approval");
        lines.push(`approval_ok\t${Boolean(fm.approval_ok) ? "yes" : "no"}`);
        lines.push(`approval_reason\t${String(fm.approval_reason ?? "-")}`);
        lines.push(`recommended_live_mode\t${String(fm.recommended_live_mode ?? "-")}`);
        lines.push(`model_approval_status\t${String(fm.model_approval_status ?? "-")}`);
        lines.push(`deploy_allowed\t${Boolean(fm.deploy_allowed) ? "yes" : "no"}`);
        lines.push(`live_trade_allowed\t${Boolean(fm.live_trade_allowed) ? "yes" : "no"}`);
        lines.push(`long_allowed\t${Boolean(fm.long_allowed) ? "yes" : "no"}`);
        lines.push(`short_allowed\t${Boolean(fm.short_allowed) ? "yes" : "no"}`);
        lines.push(`test_gated_confidence_threshold_used\t${fmt(Number(fm.test_gated_confidence_threshold_used ?? 0), 4)}`);
        lines.push(`test_gated_action_rate\t${fmt(Number(fm.test_gated_action_rate ?? 0), 6)}`);
        lines.push(`test_gated_directional_samples\t${Number(fm.test_gated_directional_samples ?? 0)}`);
        lines.push(`test_gated_pnl_proxy_after_cost\t${fmt(Number(fm.test_gated_pnl_proxy_after_cost ?? 0), 6)}`);
        lines.push(`test_gated_avg_trade_return_after_cost\t${fmt(Number(fm.test_gated_avg_trade_return_after_cost ?? 0), 6)}`);
        lines.push(`long_precision\t${fmt(Number(fm.long_precision ?? 0), 6)}`);
        lines.push(`short_precision\t${fmt(Number(fm.short_precision ?? 0), 6)}`);
        lines.push(`false_long_rate\t${fmt(Number(fm.false_long_rate ?? 0), 6)}`);
        lines.push(`false_short_rate\t${fmt(Number(fm.false_short_rate ?? 0), 6)}`);
      }
      const wfRows = Array.isArray(fm.walk_forward_folds) ? (fm.walk_forward_folds as Array<Record<string, unknown>>) : [];
      if (Boolean(fm.walk_forward_enabled) || wfRows.length) {
        lines.push("Walk-Forward Summary");
        lines.push(`walk_forward_enabled\t${Boolean(fm.walk_forward_enabled) ? "yes" : "no"}`);
        lines.push(`walk_forward_fold_count\t${Number(fm.walk_forward_fold_count ?? wfRows.length ?? 0)}`);
        lines.push(`walk_forward_test_pnl_after_cost_avg\t${fmt(Number(fm.walk_forward_test_pnl_after_cost_avg ?? 0), 6)}`);
        lines.push(`walk_forward_test_pnl_after_cost_worst\t${fmt(Number(fm.walk_forward_test_pnl_after_cost_worst ?? 0), 6)}`);
        lines.push(`walk_forward_pass_count\t${Number(fm.walk_forward_pass_count ?? 0)}`);
        lines.push(`walk_forward_fail_count\t${Number(fm.walk_forward_fail_count ?? 0)}`);
        lines.push(`walk_forward_approval_ok\t${Boolean(fm.walk_forward_approval_ok) ? "yes" : "no"}`);
        const wfReasons = Array.isArray(fm.walk_forward_rejection_reasons) ? (fm.walk_forward_rejection_reasons as Array<unknown>).map((v) => String(v)).join("|") : "";
        lines.push(`walk_forward_rejection_reasons\t${wfReasons || "-"}`);
        if (wfRows.length) {
          lines.push(
            "walk_forward_folds_table\tfold_index|selected_threshold|val_pnl_proxy_after_cost|test_pnl_proxy_after_cost|test_action_rate|test_long_precision|test_short_precision|test_false_long_rate|test_false_short_rate|test_directional_samples|pass|fail_reasons",
          );
          for (const wf of wfRows) {
            const failReasons = Array.isArray(wf.fail_reasons) ? (wf.fail_reasons as Array<unknown>).map((v) => String(v)).join("|") : "";
            lines.push(
              `walk_forward_fold\t${Number(wf.fold_index ?? 0)}|${fmt(Number(wf.selected_threshold ?? 0), 4)}|${fmt(Number(wf.val_pnl_proxy_after_cost ?? 0), 6)}|${fmt(Number(wf.test_pnl_proxy_after_cost ?? 0), 6)}|${fmt(Number(wf.test_action_rate ?? 0), 6)}|${fmt(Number(wf.test_long_precision ?? 0), 6)}|${fmt(Number(wf.test_short_precision ?? 0), 6)}|${fmt(Number(wf.test_false_long_rate ?? 0), 6)}|${fmt(Number(wf.test_false_short_rate ?? 0), 6)}|${Number(wf.test_directional_samples ?? 0)}|${Boolean(wf.pass) ? "yes" : "no"}|${failReasons || "-"}`,
            );
          }
        }
      }
      lines.push("");
      return lines;
    };

    if (summaryExportFormat === "txt") {
      const lines: string[] = [];
      lines.push(`# Epoch Summary Export`);
      lines.push(`# Run ID\t${selectedRunId || "-"}`);
      lines.push(`# Scope\t${summaryExportScope === "all_splits" ? "all_splits" : selectedSplit}`);
      lines.push(`# Rows\t${rows.length}`);
      lines.push("");
      for (const raw of rows) lines.push(...formatSummaryTextBlock(raw as Record<string, unknown>));
      downloadBlob(`${baseName}.txt`, "text/plain;charset=utf-8", lines.join("\n"));
      return;
    }
    const csv: string[] = [];
    csv.push(`"section","key","value","split","epoch"`);
    for (const raw of rows) {
      const row = raw as Record<string, unknown>;
      const split = String(row.split ?? "test").replaceAll('"', '""');
      const epoch = Number(row.epoch ?? 0);
      const lines = formatSummaryTextBlock(row);
      for (const ln of lines) {
        if (!ln.trim()) continue;
        const [k, ...rest] = ln.split("\t");
        const v = rest.join("\t");
        csv.push(`"epoch_summary","${String(k).replaceAll('"', '""')}","${String(v).replaceAll('"', '""')}","${split}",${epoch}`);
      }
    }
    downloadBlob(`${baseName}.csv`, "text/csv;charset=utf-8", csv.join("\n"));
  }

  function exportTraceViewerRows() {
    if (!traceViewerRows.length) return;
    const totalPages = Math.max(1, Math.ceil(traceViewerRows.length / TRACE_VIEWER_PAGE_SIZE));
    const pageStart = traceViewerPage * TRACE_VIEWER_PAGE_SIZE;
    let exportRows = traceViewerRows;
    let idxOffset = 0;
    if (traceExportScope === "current_page") {
      exportRows = traceViewerRows.slice(pageStart, pageStart + TRACE_VIEWER_PAGE_SIZE);
      idxOffset = pageStart;
    } else if (traceExportScope === "range") {
      const fromPage = Math.max(1, Math.min(totalPages, Math.floor(Number(traceExportFromPage) || 1)));
      const toPageRaw = Math.max(1, Math.min(totalPages, Math.floor(Number(traceExportToPage) || fromPage)));
      const toPage = Math.max(fromPage, toPageRaw);
      const fromIdx = (fromPage - 1) * TRACE_VIEWER_PAGE_SIZE;
      const toExclusive = toPage * TRACE_VIEWER_PAGE_SIZE;
      exportRows = traceViewerRows.slice(fromIdx, toExclusive);
      idxOffset = fromIdx;
    }
    if (!exportRows.length) return;
    const baseName = `trace_${selectedRunId || "run"}_${selectedSplit}_epoch${selectedEpoch}`;
    const summary = (selectedEpochSummary as Record<string, unknown>) ?? {};
    const summaryActionCounts = (summary.action_counts as Record<string, unknown>) ?? {};
    const summaryBlockedReasons = (summary.blocked_reasons as Record<string, unknown>) ?? {};
    const summaryPredDist = (summary.predicted_class_distribution as Record<string, unknown>) ?? {};
    const headers = [
      "idx",
      "action",
      "actual_label",
      "predicted_label",
      "price_at_prediction",
      "predicted_magnitude_pct",
      "predicted_abs_move_pct",
      "actual_future_return_pct",
      "prob_down",
      "prob_flat",
      "prob_up",
      "confidence",
      "confidence_gap",
      "quality",
      "entry_allowed",
      "blocked_reason",
      "gate_result",
    ];
    if (traceExportFormat === "txt") {
      const lines: string[] = [];
      if (traceExportIncludeSummary) {
        const summaryConfHist =
          (summary.confidence_distribution_histogram as Array<Record<string, unknown>> | undefined) ?? [];
        const summarySpread = (summary.confidence_spread_stats as Record<string, unknown> | undefined) ?? {};
        const summaryByLabel = (summary.future_return_distribution_by_label as Record<string, unknown> | undefined) ?? {};
        lines.push(`# Trace Export Summary`);
        lines.push(`# Run ID\t${selectedRunId || "-"}`);
        lines.push(`# Split\t${selectedSplit}`);
        lines.push(`# Epoch\t${Number(summary.epoch ?? selectedEpoch ?? 0)}`);
        lines.push(`# Samples\t${Number(summary.samples ?? traceViewerRows.length).toLocaleString()}`);
        lines.push(`# Entry Allowed Rate\t${(Number(summary.entry_allowed_rate ?? 0) * 100).toFixed(2)}%`);
        lines.push(`# Directional Pred Rate\t${(Number(summary.directional_prediction_rate ?? 0) * 100).toFixed(2)}%`);
        lines.push(`# Directional Action Rate\t${(Number(summary.directional_action_rate ?? 0) * 100).toFixed(2)}%`);
        lines.push(
          `# Actions (L/S/H)\t${Number(summaryActionCounts.LONG ?? 0).toLocaleString()} / ${Number(summaryActionCounts.SHORT ?? 0).toLocaleString()} / ${Number(summaryActionCounts.HOLD ?? 0).toLocaleString()}`,
        );
        lines.push(
          `# Predicted Class Dist\tUP: ${Number(summaryPredDist.up_count ?? 0).toLocaleString()} (${(Number(summaryPredDist.up_pct ?? 0) * 100).toFixed(2)}%), FLAT: ${Number(summaryPredDist.flat_count ?? 0).toLocaleString()} (${(Number(summaryPredDist.flat_pct ?? 0) * 100).toFixed(2)}%), DOWN: ${Number(summaryPredDist.down_count ?? 0).toLocaleString()} (${(Number(summaryPredDist.down_pct ?? 0) * 100).toFixed(2)}%)`,
        );
        lines.push(
          `# Blocked Reasons\t${
            Object.entries(summaryBlockedReasons)
              .map(([k, v]) => `${k}: ${Number(v).toLocaleString()}`)
              .join(", ") || "none"
          }`,
        );
        lines.push(
          `# Confidence Spread\tmean_top1=${fmt(Number(summarySpread.mean_top1_prob ?? 0), 4)} mean_top2=${fmt(Number(summarySpread.mean_top2_prob ?? 0), 4)} mean_gap=${fmt(Number(summarySpread.mean_confidence_gap ?? 0), 4)}`,
        );
        lines.push(
          `# Confidence Histogram\t${
            summaryConfHist.length
              ? summaryConfHist
                  .map((h) => `${String(h.bucket ?? "-")}: ${Number(h.count ?? 0).toLocaleString()} (${(Number(h.pct ?? 0) * 100).toFixed(2)}%)`)
                  .join(" | ")
              : "none"
          }`,
        );
        for (const labelKey of ["up", "flat", "down"]) {
          const row = ((summaryByLabel as Record<string, unknown>)[labelKey] as Record<string, unknown> | undefined) ?? {};
          lines.push(
            `# Future Return ${labelKey.toUpperCase()}\tcount=${Number(row.count ?? 0).toLocaleString()} mean=${fmt(Number(row.mean ?? 0), 4)} median=${fmt(Number(row.median ?? 0), 4)} p25=${fmt(Number(row.p25 ?? 0), 4)} p75=${fmt(Number(row.p75 ?? 0), 4)} min=${fmt(Number(row.min ?? 0), 4)} max=${fmt(Number(row.max ?? 0), 4)}`,
          );
        }
        lines.push("");
      }
      lines.push(headers.join("\t"));
      for (let i = 0; i < exportRows.length; i += 1) {
        const row = exportRows[i];
        lines.push(
          [
            String(i + idxOffset),
            String(row.action ?? "-"),
            String(row.actual_label ?? "-"),
            String(row.predicted_label ?? "-"),
            fmt(row.price_at_prediction, 8),
            fmt(row.predicted_magnitude_pct ?? row.magnitude_pct, 6),
            fmt(row.predicted_abs_move_pct, 6),
            fmt(row.actual_future_return_pct, 6),
            fmt(row.prob_down, 6),
            fmt(row.prob_flat, 6),
            fmt(row.prob_up, 6),
            fmt(row.confidence, 6),
            fmt(row.confidence_gap, 6),
            fmt(row.quality, 6),
            String(row.entry_allowed ?? false),
            String(row.blocked_reason ?? ""),
            String(row.gate_result ?? ""),
          ].join("\t"),
        );
      }
      downloadBlob(`${baseName}.txt`, "text/plain;charset=utf-8", lines.join("\n"));
      return;
    }
    const csvLines: string[] = [];
    if (traceExportIncludeSummary) {
      const summaryConfHist =
        (summary.confidence_distribution_histogram as Array<Record<string, unknown>> | undefined) ?? [];
      const summarySpread = (summary.confidence_spread_stats as Record<string, unknown> | undefined) ?? {};
      const summaryByLabel = (summary.future_return_distribution_by_label as Record<string, unknown> | undefined) ?? {};
      csvLines.push(`"summary_key","summary_value"`);
      csvLines.push(`"run_id","${String(selectedRunId || "-").replaceAll('"', '""')}"`);
      csvLines.push(`"split","${String(selectedSplit).replaceAll('"', '""')}"`);
      csvLines.push(`"epoch","${Number(summary.epoch ?? selectedEpoch ?? 0)}"`);
      csvLines.push(`"samples","${Number(summary.samples ?? traceViewerRows.length)}"`);
      csvLines.push(`"entry_allowed_rate_pct","${(Number(summary.entry_allowed_rate ?? 0) * 100).toFixed(2)}"`);
      csvLines.push(`"directional_prediction_rate_pct","${(Number(summary.directional_prediction_rate ?? 0) * 100).toFixed(2)}"`);
      csvLines.push(`"directional_action_rate_pct","${(Number(summary.directional_action_rate ?? 0) * 100).toFixed(2)}"`);
      csvLines.push(`"actions_lsh","${Number(summaryActionCounts.LONG ?? 0)}/${Number(summaryActionCounts.SHORT ?? 0)}/${Number(summaryActionCounts.HOLD ?? 0)}"`);
      csvLines.push(
        `"predicted_class_dist","UP:${Number(summaryPredDist.up_count ?? 0)}(${(Number(summaryPredDist.up_pct ?? 0) * 100).toFixed(2)}%) FLAT:${Number(summaryPredDist.flat_count ?? 0)}(${(Number(summaryPredDist.flat_pct ?? 0) * 100).toFixed(2)}%) DOWN:${Number(summaryPredDist.down_count ?? 0)}(${(Number(summaryPredDist.down_pct ?? 0) * 100).toFixed(2)}%)"`,
      );
      csvLines.push(
        `"blocked_reasons","${
          Object.entries(summaryBlockedReasons)
            .map(([k, v]) => `${k}:${Number(v)}`)
            .join("; ") || "none"
        }"`,
      );
      csvLines.push(`"mean_top1_prob","${fmt(Number(summarySpread.mean_top1_prob ?? 0), 6)}"`);
      csvLines.push(`"mean_top2_prob","${fmt(Number(summarySpread.mean_top2_prob ?? 0), 6)}"`);
      csvLines.push(`"mean_confidence_gap","${fmt(Number(summarySpread.mean_confidence_gap ?? 0), 6)}"`);
      csvLines.push(
        `"confidence_histogram","${
          summaryConfHist.length
            ? summaryConfHist
                .map((h) => `${String(h.bucket ?? "-")}:${Number(h.count ?? 0)}(${(Number(h.pct ?? 0) * 100).toFixed(2)}%)`)
                .join(" | ")
            : "none"
        }"`,
      );
      for (const labelKey of ["up", "flat", "down"]) {
        const row = ((summaryByLabel as Record<string, unknown>)[labelKey] as Record<string, unknown> | undefined) ?? {};
        csvLines.push(`"future_return_${labelKey}_count","${Number(row.count ?? 0)}"`);
        csvLines.push(`"future_return_${labelKey}_mean","${fmt(Number(row.mean ?? 0), 6)}"`);
        csvLines.push(`"future_return_${labelKey}_median","${fmt(Number(row.median ?? 0), 6)}"`);
        csvLines.push(`"future_return_${labelKey}_p25","${fmt(Number(row.p25 ?? 0), 6)}"`);
        csvLines.push(`"future_return_${labelKey}_p75","${fmt(Number(row.p75 ?? 0), 6)}"`);
        csvLines.push(`"future_return_${labelKey}_min","${fmt(Number(row.min ?? 0), 6)}"`);
        csvLines.push(`"future_return_${labelKey}_max","${fmt(Number(row.max ?? 0), 6)}"`);
      }
      csvLines.push("");
    }
    csvLines.push(headers.join(","));
    for (let i = 0; i < exportRows.length; i += 1) {
      const row = exportRows[i];
      csvLines.push(
        [
          i + idxOffset,
          `"${String(row.action ?? "-").replaceAll('"', '""')}"`,
          `"${String(row.actual_label ?? "-").replaceAll('"', '""')}"`,
          `"${String(row.predicted_label ?? "-").replaceAll('"', '""')}"`,
          fmt(row.price_at_prediction, 8),
          fmt(row.predicted_magnitude_pct ?? row.magnitude_pct, 6),
          fmt(row.predicted_abs_move_pct, 6),
          fmt(row.actual_future_return_pct, 6),
          fmt(row.prob_down, 6),
          fmt(row.prob_flat, 6),
          fmt(row.prob_up, 6),
          fmt(row.confidence, 6),
          fmt(row.confidence_gap, 6),
          fmt(row.quality, 6),
          String(Boolean(row.entry_allowed)),
          `"${String(row.blocked_reason ?? "").replaceAll('"', '""')}"`,
          `"${String(row.gate_result ?? "").replaceAll('"', '""')}"`,
        ].join(","),
      );
    }
    // CSV opens directly in Excel.
    downloadBlob(`${baseName}.csv`, "text/csv;charset=utf-8", csvLines.join("\n"));
  }

  async function copyReplayDiagnosticsSummary() {
    if (!selectedEpochSummary) {
      setError("No epoch summary is available to copy.");
      return;
    }
    const summary = (selectedEpochSummary as Record<string, unknown>) ?? {};
    const lines: string[] = [];
    lines.push(`Replay state\t${replayStatus.toUpperCase()} | split ${selectedSplit} | frame ${(replayStep?.cursor ?? 0) + 1}/${replayStep?.frames_total ?? 0}`);
    lines.push(`Replay source\t${replaySourceMode === "market_event_replay" ? "market_event_replay (full-fidelity chunks)" : "prediction_trace"}`);
    lines.push("");
    lines.push("Epoch Summary");
    lines.push(`Epoch\t${Number(summary.epoch ?? 0)}`);
    lines.push(`Samples\t${Number(summary.samples ?? 0).toLocaleString()}`);
    lines.push(`Accuracy\t${(Number(summary.accuracy ?? 0) * 100).toFixed(2)}%`);
    lines.push(`Entry Allowed Rate\t${(Number(summary.entry_allowed_rate ?? 0) * 100).toFixed(2)}%`);
    lines.push(`Blocked Count\t${Number(summary.blocked_count ?? 0).toLocaleString()}`);
    lines.push(`Directional Pred Rate\t${(Number(summary.directional_prediction_rate ?? 0) * 100).toFixed(2)}%`);
    lines.push(`Directional Action Rate\t${(Number(summary.directional_action_rate ?? 0) * 100).toFixed(2)}%`);
    lines.push(`Abstention Rate\t${(Number(summary.abstention_rate ?? 0) * 100).toFixed(2)}%`);
    lines.push(`Abstention Correctness\t${(Number(summary.abstention_correctness ?? 0) * 100).toFixed(2)}%`);
    const actionCounts = (summary.action_counts as Record<string, unknown>) ?? {};
    lines.push(
      `Actions (L/S/H)\t${Number(actionCounts.LONG ?? 0).toLocaleString()} / ${Number(actionCounts.SHORT ?? 0).toLocaleString()} / ${Number(actionCounts.HOLD ?? 0).toLocaleString()}`,
    );
    const predDist = (summary.predicted_class_distribution as Record<string, unknown>) ?? {};
    lines.push(
      `Predicted Class Dist\tUP: ${Number(predDist.up_count ?? 0).toLocaleString()} (${(Number(predDist.up_pct ?? 0) * 100).toFixed(2)}%), FLAT: ${Number(predDist.flat_count ?? 0).toLocaleString()} (${(Number(predDist.flat_pct ?? 0) * 100).toFixed(2)}%), DOWN: ${Number(predDist.down_count ?? 0).toLocaleString()} (${(Number(predDist.down_pct ?? 0) * 100).toFixed(2)}%)`,
    );
    const blockedReasons = (summary.blocked_reasons as Record<string, unknown>) ?? {};
    lines.push(
      `Blocked Reasons\t${Object.entries(blockedReasons).map(([k, v]) => `${k}: ${Number(v).toLocaleString()}`).join(", ") || "none"}`,
    );
    lines.push(`Gate Paralyzed\t${Boolean(summary.gate_paralyzed) ? "yes" : "no"}`);
    lines.push("");
    lines.push("Trade Rate by Class");
    const tr = (summary.trade_rate_by_class as Record<string, unknown>) ?? {};
    lines.push(`Long rate when actual up\t${(Number(tr.long_rate_when_actual_up ?? 0) * 100).toFixed(2)}%`);
    lines.push(`Short rate when actual down\t${(Number(tr.short_rate_when_actual_down ?? 0) * 100).toFixed(2)}%`);
    lines.push(`False long rate\t${(Number(tr.false_long_rate ?? 0) * 100).toFixed(2)}%`);
    lines.push(`False short rate\t${(Number(tr.false_short_rate ?? 0) * 100).toFixed(2)}%`);
    lines.push("");
    lines.push("Expected Edge by Confidence Bucket");
    const edgeBuckets = Array.isArray(summary.expected_edge_by_confidence_bucket)
      ? (summary.expected_edge_by_confidence_bucket as Array<Record<string, unknown>>)
      : [];
    if (!edgeBuckets.length) {
      lines.push("none");
    } else {
      for (const row of edgeBuckets) {
        lines.push(
          `${String(row.bucket ?? "-")}\tcount=${Number(row.sample_count ?? 0)} avg=${fmt(Number(row.avg_future_return_pct ?? 0), 6)} median=${fmt(Number(row.median_future_return_pct ?? 0), 6)} hit=${(Number(row.direction_hit_rate ?? 0) * 100).toFixed(2)}%`,
        );
      }
    }
    lines.push("");
    lines.push("Confidence Distribution");
    const hist = Array.isArray(summary.confidence_distribution_histogram)
      ? (summary.confidence_distribution_histogram as Array<Record<string, unknown>>)
      : [];
    if (!hist.length) {
      lines.push("none");
    } else {
      for (const h of hist) {
        lines.push(`${String(h.bucket ?? "-")}\tcount=${Number(h.count ?? 0)} pct=${(Number(h.pct ?? 0) * 100).toFixed(2)}%`);
      }
    }
    lines.push("");
    lines.push("Probability Spread");
    lines.push(`Mean top1 prob\t${fmt(Number(summary.mean_top1_prob ?? 0), 6)}`);
    lines.push(`Mean top2 prob\t${fmt(Number(summary.mean_top2_prob ?? 0), 6)}`);
    lines.push(`Mean confidence gap\t${fmt(Number(summary.mean_confidence_gap ?? 0), 6)}`);
    lines.push("");
    lines.push("Future Return Distribution by Label");
    const byLabel = (summary.future_return_distribution_by_label as Record<string, unknown>) ?? {};
    for (const key of ["up", "flat", "down"]) {
      const row = ((byLabel as Record<string, unknown>)[key] as Record<string, unknown>) ?? {};
      lines.push(
        `${key.toUpperCase()}\tcount=${Number(row.count ?? 0)} mean=${fmt(Number(row.mean ?? 0), 6)} median=${fmt(Number(row.median ?? 0), 6)} p25=${fmt(Number(row.p25 ?? 0), 6)} p75=${fmt(Number(row.p75 ?? 0), 6)} min=${fmt(Number(row.min ?? 0), 6)} max=${fmt(Number(row.max ?? 0), 6)}`,
      );
    }
    lines.push("");
    const fs = ((metrics?.final_metrics ?? {}) as Record<string, unknown>).feature_separability as Record<string, unknown> | undefined;
    lines.push("Feature Separability");
    if (!fs || typeof fs !== "object") {
      lines.push("none");
    } else {
      for (const [name, raw] of Object.entries(fs)) {
        if (!raw || typeof raw !== "object") continue;
        const obj = raw as Record<string, unknown>;
        const byLabel = (obj.by_label as Record<string, unknown>) ?? {};
        const sep = (obj.separability as Record<string, unknown>) ?? {};
        const up = (byLabel.up as Record<string, unknown>) ?? {};
        const flat = (byLabel.flat as Record<string, unknown>) ?? {};
        const down = (byLabel.down as Record<string, unknown>) ?? {};
        lines.push(
          `${name}\tup=${fmt(Number(up.mean ?? 0), 4)} flat=${fmt(Number(flat.mean ?? 0), 4)} down=${fmt(Number(down.mean ?? 0), 4)} sep(U-D)=${fmt(Number(sep.up_down ?? 0), 3)} sep(U-F)=${fmt(Number(sep.up_flat ?? 0), 3)} sep(F-D)=${fmt(Number(sep.flat_down ?? 0), 3)} corr=${fmt(Number(obj.corr_with_future_return ?? 0), 3)}`,
        );
      }
    }
    const fm = (metrics?.final_metrics ?? {}) as Record<string, unknown>;
    const spacing = (fm.triple_barrier_step_spacing_stats as Record<string, unknown>) ?? {};
    const pathProbe = Array.isArray(fm.triple_barrier_future_path_probe_rows)
      ? (fm.triple_barrier_future_path_probe_rows as Array<Record<string, unknown>>)
      : [];
    if (Object.keys(spacing).length || pathProbe.length) {
      lines.push("");
      lines.push("Triple-Barrier Step Spacing");
      lines.push(`median_step_delta_ms\t${fmt(Number(spacing.median_step_delta_ms ?? 0), 3)}`);
      lines.push(`min_step_delta_ms\t${fmt(Number(spacing.min_step_delta_ms ?? 0), 3)}`);
      lines.push(`max_step_delta_ms\t${fmt(Number(spacing.max_step_delta_ms ?? 0), 3)}`);
      lines.push(`estimated_timeout_seconds\t${fmt(Number(fm.estimated_timeout_seconds ?? spacing.estimated_timeout_seconds ?? 0), 3)}`);
      lines.push(`non_monotonic_timestamp_count\t${Number(spacing.non_monotonic_timestamp_count ?? 0)}`);
      lines.push(`contiguous_within_tolerance\t${Boolean(spacing.contiguous_within_tolerance) ? "yes" : "no"}`);
      lines.push("");
      lines.push("Triple-Barrier Future Path Probe (first 5)");
      for (const row of pathProbe.slice(0, 5)) {
        lines.push(
          `entry=${String(row.entry_price ?? "-")} | future_prices_0_20=${JSON.stringify(row.future_prices_0_20 ?? [])} | future_timestamps_0_20=${JSON.stringify(row.future_timestamps_0_20 ?? [])} | time_delta_ms_0_20=${JSON.stringify(row.time_delta_ms_0_20 ?? [])}`,
        );
      }
    }
    const rejRate = Number(fm.label_price_rejected_rate ?? 0);
    const rejCnt = Number(fm.label_price_rejected_count ?? 0);
    const rejTot = Number(fm.bookticker_updates_total ?? 0);
    const rejThr = Number(fm.max_label_price_rejected_rate_used ?? 0.005);
    lines.push("");
    lines.push("Label-Price Quality Guard");
    lines.push(`source\t${String(fm.label_price_source_used ?? "mid_price")}`);
    lines.push(`rejected_rate\t${(rejRate * 100).toFixed(4)}%`);
    lines.push(`rejected_count\t${rejCnt}`);
    lines.push(`bookticker_updates_total\t${rejTot}`);
    lines.push(`threshold\t${(rejThr * 100).toFixed(4)}%`);
    lines.push(`guard_triggered\t${Boolean(fm.label_price_quality_guard_triggered) ? "yes" : "no"}`);
    const text = lines.join("\n");
    try {
      await navigator.clipboard.writeText(text);
      setError("");
    } catch {
      setError("Could not copy summary to clipboard.");
    }
  }

  async function startReplay() {
    if (!selectedRunId) return;
    setLoading(true);
    setError("");
    try {
      const res = await fetch(`${API_ROOT}/ml/replay/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          run_id: selectedRunId,
          mode: "test_trace",
          speed: replaySpeed,
          epoch_index: selectedEpoch,
          split: selectedSplit,
          source_mode: replaySourceMode,
        }),
      });
      const data = (await res.json()) as { ok?: boolean; replay_id?: string; reason?: string };
      if (data.ok && data.replay_id) {
        onReplaySessionChange(data.replay_id);
        setReplayStatus("paused");
        setReplayParityStats(null);
      } else {
        setError(friendlyApiError(data.reason, "Replay trace unavailable."));
      }
    } catch {
      setError("Failed to start replay.");
    } finally {
      setLoading(false);
    }
  }

  async function controlReplay(
    action: "play" | "pause" | "close",
    cursor?: number,
    epochIndex?: number,
    split?: "train" | "val" | "test",
    sourceModeOverride?: ReplaySourceMode,
    speedOverride?: number,
  ) {
    if (!replaySessionId && action !== "close") return;
    try {
      const effectiveSourceMode = sourceModeOverride ?? replaySourceMode;
      if (replaySessionId) {
        const speedPayload = Number.isFinite(Number(speedOverride ?? replaySpeed)) ? Number(speedOverride ?? replaySpeed) : 1;
        await fetch(`${API_ROOT}/ml/replay/${replaySessionId}/control`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action, cursor, speed: speedPayload, epoch_index: epochIndex, split, source_mode: effectiveSourceMode }),
        });
      }
      if (action === "close") {
        onReplaySessionChange("");
        setReplayStep(null);
        setReplayStatus("paused");
        setReplayHistoryFrames([]);
        lastNonEmptyReplayHistoryRef.current = [];
        setReplayParityStats(null);
        replayHistoryCacheRef.current.clear();
        setReplayTerminalState(buildReplayTerminalState(symbol, null, null, replayBaseCandles, null, replayGlobalCandleBubbles, fullChartMode));
        return;
      }
      if (replaySessionId) {
        const stepRes = await fetch(`${API_ROOT}/ml/replay/${replaySessionId}/step`);
        const stepData = (await stepRes.json()) as ReplayStepFrame;
        if (isReplayStepFrame(stepData)) {
          const nextMode = normalizeReplaySourceMode(stepData.source_mode);
          const eventHistory = nextMode === "market_event_replay"
            ? []
            : await loadReplayHistoryCached(
                replayHistoryCacheRef,
                replaySessionId,
                Number(stepData.cursor ?? 0),
                Number(stepData.frames_total ?? 0),
                replayCandleLimit,
                String(stepData.split ?? split ?? selectedSplit),
                Number(stepData.epoch_index ?? epochIndex ?? selectedEpoch),
              );
          setReplayStep(stepData);
          setTimelineCursor(Math.max(0, Number(stepData.cursor ?? 0)));
          const effectiveHistory = eventHistory.length > 0 ? eventHistory : lastNonEmptyReplayHistoryRef.current;
          if (effectiveHistory.length > 0) {
            lastNonEmptyReplayHistoryRef.current = effectiveHistory;
          }
          setReplayHistoryFrames(effectiveHistory);
          setReplayTerminalState((prev) =>
            buildReplayTerminalState(symbol, stepData, prev, replayBaseCandles, effectiveHistory, replayGlobalCandleBubbles, fullChartMode),
          );
          setReplaySourceMode(nextMode);
          if (nextMode === "market_event_replay") {
            void fetchReplayParityStats(replaySessionId);
          } else {
            setReplayParityStats(null);
          }
          setError("");
        } else {
          setReplayStep(null);
          setError("Replay step unavailable.");
        }
      }
      setReplayStatus(action === "play" ? "playing" : "paused");
    } catch {
      setReplayStatus("paused");
      setError("Replay control failed. If this replay is expired, click Start Replay again.");
    }
  }

  function queueSeek(nextCursor: number, immediate = false) {
    const clamped = Math.max(0, Math.min(nextCursor, Math.max(0, (replayStep?.frames_total ?? 1) - 1)));
    setTimelineCursor(clamped);
    if (seekTimerRef.current) {
      window.clearTimeout(seekTimerRef.current);
      seekTimerRef.current = null;
    }
    if (immediate) {
      void controlReplay("pause", clamped);
      return;
    }
    seekTimerRef.current = window.setTimeout(() => {
      seekTimerRef.current = null;
      void controlReplay("pause", clamped);
    }, 160);
  }

  return (
    <>
      <section className="panel" style={{ marginBottom: 10 }}>
        <div className="small-title">Replay Controls</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <div className="watchlist-inline" style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              className={activeTerminalMode === "live" ? "active" : ""}
              onClick={() => onTerminalModeChange?.("live")}
            >
              Live Terminal
            </button>
            <button
              type="button"
              className={activeTerminalMode === "replay" ? "active" : ""}
              onClick={() => onTerminalModeChange?.("replay")}
            >
              Replay Terminal
            </button>
          </div>
          <span>{symbol}</span>
          <select value={selectedRunId} onChange={(e) => setSelectedRunId(e.target.value)}>
            <option value="">Select run</option>
            {runs.map((run) => (
              <option key={run.run_id} value={run.run_id}>
                {run.pair_symbol} | {run.run_id} | {run.status} | {formatRunDateTime(run.created_at)}
              </option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => void startReplay()}
            disabled={!selectedRunId || loading || (replayTraceRequired && !epochRecordedForSelectedSplit)}
            title={replayTraceRequired && !epochRecordedForSelectedSplit ? notRecordedMessage : ""}
          >
            {loading ? "Starting..." : "Start Replay"}
          </button>
          <select
            value={String(selectedEpoch)}
            onChange={(e) => {
              const next = Number(e.target.value || 0);
              setSelectedEpoch(next);
              if (replaySessionId) void controlReplay("pause", 0, next, selectedSplit);
            }}
          >
            <option value="0">Final trace</option>
            {capturedEpochs.map((ep) => (
              <option key={`ep-${ep}`} value={ep}>
                Epoch {ep}
              </option>
            ))}
          </select>
          <select
            value={selectedSplit}
            onChange={(e) => {
              const next = (e.target.value === "train" || e.target.value === "val" ? e.target.value : "test") as "train" | "val" | "test";
              setSelectedSplit(next);
              if (replaySessionId) void controlReplay("pause", 0, selectedEpoch, next);
            }}
          >
            <option value="test">Test split</option>
            <option value="val">Val split</option>
            <option value="train">Train split</option>
          </select>
          <select
            value={replaySourceMode}
            onChange={(e) => {
              const nextMode = normalizeReplaySourceMode(e.target.value);
              setReplaySourceMode(nextMode);
              if (replaySessionId) void controlReplay("pause", 0, selectedEpoch, selectedSplit, nextMode);
            }}
            title="Replay source mode"
          >
            <option value="prediction_trace">Source: Prediction Trace</option>
            <option value="market_event_replay">Source: Market Event Replay</option>
          </select>
          <select
            value={String(replaySpeed)}
            onChange={(e) => {
              const next = Number(e.target.value || 1);
              const allowed = [1, 2, 4, 6];
              const chosen = allowed.includes(next) ? next : 1;
              setReplaySpeed(chosen);
              if (replaySessionId) void controlReplay("pause", undefined, undefined, undefined, undefined, chosen);
            }}
            title="Replay speed"
          >
            <option value="1">Speed: Normal</option>
            <option value="2">Speed: 2x</option>
            <option value="4">Speed: 4x</option>
            <option value="6">Speed: 6x</option>
          </select>
          <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={fullChartMode} onChange={(e) => setFullChartMode(e.target.checked)} />
            Full Chart+Bubbles
          </label>
          <button type="button" onClick={() => void controlReplay("play")} disabled={!replaySessionId}>Play</button>
          <button type="button" onClick={() => void controlReplay("pause")} disabled={!replaySessionId}>Pause</button>
          <button
            type="button"
            onClick={() => void controlReplay("pause", Math.min((replayStep?.cursor ?? 0) + 1, Math.max(0, (replayStep?.frames_total ?? 1) - 1)))}
            disabled={!replaySessionId}
          >
            Step
          </button>
          <button
            type="button"
            onClick={() => void controlReplay("pause", Math.max(0, (replayStep?.frames_total ?? 1) - 1))}
            disabled={!replaySessionId}
          >
            End
          </button>
          <button type="button" onClick={() => void controlReplay("close")} disabled={!replaySessionId}>Close Replay</button>
        </div>
        <div className="kv">
          <span>Replay state</span>
          <span>{replayStatus.toUpperCase()} | split {selectedSplit} | frame {(replayStep?.cursor ?? 0) + 1}/{replayStep?.frames_total ?? 0}</span>
        </div>
        <div className="kv">
          <span>Replay source</span>
          <span>{replaySourceMode === "market_event_replay" ? "market_event_replay (full-fidelity chunks)" : "prediction_trace"}</span>
        </div>
        {fullChartMode ? (
          <div className="kv">
            <span>Full chart span</span>
            <span>{replayLoadedSpanText}</span>
          </div>
        ) : null}
        {fullChartMode ? (
          <div className="kv">
            <span>Trace rows loaded</span>
            <span>{replayGlobalTraceRows.length.toLocaleString()}</span>
          </div>
        ) : null}
        {replaySourceMode === "market_event_replay" && replayParityStats ? (
          <div className="kv">
            <span>Parity stats</span>
            <span>
              events {Number(replayParityStats.processed_events ?? 0).toLocaleString()}/{Number(replayParityStats.expected_events ?? 0).toLocaleString()}
              {" | "}
              batch {Number(replayParityStats.batch_ms ?? 0)}ms
            </span>
          </div>
        ) : null}
        {replayTraceRequired && !epochRecordedForSelectedSplit ? <div className="pair-error">{notRecordedMessage}</div> : null}
        {replayStep ? (
          <div className="kv">
            <span>Action progress</span>
              <span>
              LONG {actionRatio(replayStep, "LONG", epochActionTotals, replaySeenFromHistory)} | SHORT {actionRatio(replayStep, "SHORT", epochActionTotals, replaySeenFromHistory)} | HOLD {actionRatio(replayStep, "HOLD", epochActionTotals, replaySeenFromHistory)}
            </span>
          </div>
        ) : null}
        {selectedFrame ? (
          <>
            <div className="kv">
              <span>Probabilities</span>
              <span>
                down {fmt(selectedFrame.prob_down, 3)} | flat {fmt(selectedFrame.prob_flat, 3)} | up {fmt(selectedFrame.prob_up, 3)}
              </span>
            </div>
            <div className="kv">
              <span>Decision details</span>
              <span>
                conf {fmt(selectedFrame.confidence, 3)} | quality {fmt(selectedFrame.quality, 3)} | gap {fmt(selectedFrame.confidence_gap, 3)}
              </span>
            </div>
            <div className="kv">
              <span>Pred vs actual move</span>
              <span>
                pred {fmt(selectedFrame.predicted_magnitude_pct ?? selectedFrame.magnitude_pct, 4)}% | actual {fmt(selectedFrame.actual_future_return_pct, 4)}%
              </span>
            </div>
            <div className="kv">
              <span>Abs predicted move</span>
              <span>{fmt(selectedFrame.predicted_abs_move_pct, 4)}%</span>
            </div>
            <div className="kv">
              <span>Price @ prediction</span>
              <span>{fmt(selectedFrame.price_at_prediction, 6)}</span>
            </div>
            <div className="kv">
              <span>Gate</span>
              <span>
                {selectedFrame.entry_allowed ? "entry_allowed" : "blocked"}
                {!selectedFrame.entry_allowed ? ` (${String(selectedFrame.blocked_reason || "other_gate")})` : ""}
                {" | "}
                correct {selectedFrame.correct ? "yes" : "no"}
              </span>
            </div>
          </>
        ) : null}
        {selectedEpochSummary ? (
          <div style={{ marginTop: 8 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div className="small-title">Epoch Summary</div>
              <button type="button" onClick={() => void copyReplayDiagnosticsSummary()}>
                Copy Summary
              </button>
              <button type="button" onClick={exportEpochSummaries}>
                Export Summaries
              </button>
              <select
                value={summaryExportFormat}
                onChange={(e) => setSummaryExportFormat(e.target.value === "txt" ? "txt" : "excel")}
                title="Epoch summary export format"
              >
                <option value="excel">Excel file (.csv)</option>
                <option value="txt">Text file (.txt)</option>
              </select>
              <select
                value={summaryExportScope}
                onChange={(e) => setSummaryExportScope(e.target.value === "all_splits" ? "all_splits" : "selected_split")}
                title="Epoch summary export scope"
              >
                <option value="selected_split">Selected split</option>
                <option value="all_splits">All splits</option>
              </select>
            </div>
            <table style={{ borderCollapse: "collapse", width: 520 }}>
              <tbody>
                <tr>
                  <td style={{ paddingRight: 12 }}>Epoch</td>
                  <td>{Number((selectedEpochSummary as Record<string, unknown>).epoch ?? 0)}</td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Samples</td>
                  <td>{Number((selectedEpochSummary as Record<string, unknown>).samples ?? 0).toLocaleString()}</td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Accuracy</td>
                  <td>{(Number((selectedEpochSummary as Record<string, unknown>).accuracy ?? 0) * 100).toFixed(2)}%</td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Entry Allowed Rate</td>
                  <td>
                    {(Number((selectedEpochSummary as Record<string, unknown>).entry_allowed_rate ?? 0) * 100).toFixed(2)}%
                  </td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Blocked Count</td>
                  <td>{Number((selectedEpochSummary as Record<string, unknown>).blocked_count ?? 0).toLocaleString()}</td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Directional Pred Rate</td>
                  <td>{(Number((selectedEpochSummary as Record<string, unknown>).directional_prediction_rate ?? 0) * 100).toFixed(2)}%</td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Directional Action Rate</td>
                  <td>{(Number((selectedEpochSummary as Record<string, unknown>).directional_action_rate ?? 0) * 100).toFixed(2)}%</td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Actions (L/S/H)</td>
                  <td>
                    {(() => {
                      const counts = ((selectedEpochSummary as Record<string, unknown>).action_counts as Record<string, unknown>) ?? {};
                      const l = Number(counts.LONG ?? 0).toLocaleString();
                      const s = Number(counts.SHORT ?? 0).toLocaleString();
                      const h = Number(counts.HOLD ?? 0).toLocaleString();
                      return `${l} / ${s} / ${h}`;
                    })()}
                  </td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12, verticalAlign: "top" }}>Predicted Class Dist</td>
                  <td>
                    {(() => {
                      const dist = ((selectedEpochSummary as Record<string, unknown>).predicted_class_distribution as Record<string, unknown>) ?? {};
                      const upCount = Number(dist.up_count ?? 0);
                      const flatCount = Number(dist.flat_count ?? 0);
                      const downCount = Number(dist.down_count ?? 0);
                      const upPct = Number(dist.up_pct ?? 0) * 100;
                      const flatPct = Number(dist.flat_pct ?? 0) * 100;
                      const downPct = Number(dist.down_pct ?? 0) * 100;
                      return `UP: ${upCount.toLocaleString()} (${upPct.toFixed(2)}%), FLAT: ${flatCount.toLocaleString()} (${flatPct.toFixed(2)}%), DOWN: ${downCount.toLocaleString()} (${downPct.toFixed(2)}%)`;
                    })()}
                  </td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12, verticalAlign: "top" }}>Blocked Reasons</td>
                  <td>
                    {Object.entries(
                      ((selectedEpochSummary as Record<string, unknown>).blocked_reasons as Record<string, unknown>) ?? {},
                    )
                      .map(([k, v]) => `${k}: ${Number(v).toLocaleString()}`)
                      .join(", ") || "none"}
                  </td>
                </tr>
                <tr>
                  <td style={{ paddingRight: 12 }}>Gate Paralyzed</td>
                  <td>{Boolean((selectedEpochSummary as Record<string, unknown>).gate_paralyzed) ? "yes" : "no"}</td>
                </tr>
              </tbody>
            </table>
            {(() => {
              const tr = ((selectedEpochSummary as Record<string, unknown>).trade_rate_by_class as Record<string, unknown>) ?? null;
              if (!tr) return null;
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Trade Rate by Class</div>
                  <div className="kv"><span>LONG when actual UP</span><span>{(Number(tr.long_rate_when_actual_up ?? 0) * 100).toFixed(2)}%</span></div>
                  <div className="kv"><span>SHORT when actual DOWN</span><span>{(Number(tr.short_rate_when_actual_down ?? 0) * 100).toFixed(2)}%</span></div>
                  <div className="kv"><span>False LONG rate</span><span>{(Number(tr.false_long_rate ?? 0) * 100).toFixed(2)}%</span></div>
                  <div className="kv"><span>False SHORT rate</span><span>{(Number(tr.false_short_rate ?? 0) * 100).toFixed(2)}%</span></div>
                </div>
              );
            })()}
            {(() => {
              const buckets = ((selectedEpochSummary as Record<string, unknown>).expected_edge_by_confidence_bucket as Array<Record<string, unknown>>) ?? [];
              if (!Array.isArray(buckets) || buckets.length === 0) return null;
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Expected Edge by Confidence Bucket</div>
                  <table style={{ borderCollapse: "collapse", width: 520 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", paddingRight: 8 }}>bucket</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>n</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>avg%</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>med%</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>hit%</th>
                      </tr>
                    </thead>
                    <tbody>
                      {buckets.map((b, i) => (
                        <tr key={`bucket-${i}`}>
                          <td style={{ paddingRight: 8 }}>{String(b.bucket ?? "-")}{Boolean(b.low_confidence_bucket) ? " *" : ""}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number(b.sample_count ?? 0).toLocaleString()}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(b.avg_future_return_pct ?? 0), 4)}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(b.median_future_return_pct ?? 0), 4)}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{(Number(b.direction_hit_rate ?? 0) * 100).toFixed(2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
            {(() => {
              const hist = ((selectedEpochSummary as Record<string, unknown>).confidence_distribution_histogram as Array<Record<string, unknown>>) ?? [];
              const spread = ((selectedEpochSummary as Record<string, unknown>).confidence_spread_stats as Record<string, unknown>) ?? null;
              if (!Array.isArray(hist) || hist.length === 0) return null;
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Confidence Distribution</div>
                  {hist.map((h, i) => (
                    <div key={`conf-h-${i}`} className="kv">
                      <span>{String(h.bucket ?? "-")}</span>
                      <span>{Number(h.count ?? 0).toLocaleString()} ({(Number(h.pct ?? 0) * 100).toFixed(2)}%)</span>
                    </div>
                  ))}
                  {spread ? (
                    <>
                      <div className="small-title" style={{ marginTop: 6 }}>Probability Spread</div>
                      <div className="kv"><span>mean top1 prob</span><span>{fmt(Number(spread.mean_top1_prob ?? 0), 4)}</span></div>
                      <div className="kv"><span>mean top2 prob</span><span>{fmt(Number(spread.mean_top2_prob ?? 0), 4)}</span></div>
                      <div className="kv"><span>mean confidence gap</span><span>{fmt(Number(spread.mean_confidence_gap ?? 0), 4)}</span></div>
                    </>
                  ) : null}
                </div>
              );
            })()}
            {(() => {
              const byLabel = ((selectedEpochSummary as Record<string, unknown>).future_return_distribution_by_label as Record<string, unknown>) ?? null;
              if (!byLabel) return null;
              const rows: Array<{ key: string; value: Record<string, unknown> }> = ["up", "flat", "down"].map((k) => ({
                key: k.toUpperCase(),
                value: ((byLabel as Record<string, unknown>)[k] as Record<string, unknown>) ?? {},
              }));
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Future Return Distribution by Label</div>
                  <table style={{ borderCollapse: "collapse", width: 700 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", paddingRight: 8 }}>label</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>count</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>mean</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>median</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>p25</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>p75</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>min</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>max</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => (
                        <tr key={`lbl-${r.key}`}>
                          <td style={{ paddingRight: 8 }}>{r.key}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number(r.value.count ?? 0).toLocaleString()}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(r.value.mean ?? 0), 4)}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(r.value.median ?? 0), 4)}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(r.value.p25 ?? 0), 4)}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(r.value.p75 ?? 0), 4)}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(r.value.min ?? 0), 4)}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{fmt(Number(r.value.max ?? 0), 4)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
            {(() => {
              const hitStats = ((selectedEpochSummary as Record<string, unknown>).triple_barrier_hit_stats as Record<string, unknown>) ?? null;
              const probeRows = ((selectedEpochSummary as Record<string, unknown>).triple_barrier_probe_rows as Array<Record<string, unknown>>) ?? [];
              if ((!hitStats || typeof hitStats !== "object") && probeRows.length === 0) return null;
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Triple-Barrier Debug Probe</div>
                  {hitStats && typeof hitStats === "object" ? (
                    <div className="kv">
                      <span>Hit stats</span>
                      <span>
                        TP-first {Number(hitStats.tp_first_count ?? 0).toLocaleString()} | SL-first {Number(hitStats.sl_first_count ?? 0).toLocaleString()} | No-hit {Number(hitStats.no_hit_count ?? 0).toLocaleString()} | Tie {Number(hitStats.tie_count ?? 0).toLocaleString()}
                      </span>
                    </div>
                  ) : null}
                  {probeRows.length ? (
                    <div style={{ overflowX: "auto" }}>
                      <table style={{ borderCollapse: "collapse", width: 980 }}>
                        <thead>
                          <tr>
                            <th style={{ textAlign: "right", paddingRight: 8 }}>entry_price</th>
                            <th style={{ textAlign: "right", paddingRight: 8 }}>tp_price</th>
                            <th style={{ textAlign: "right", paddingRight: 8 }}>sl_price</th>
                            <th style={{ textAlign: "right", paddingRight: 8 }}>future_high_max</th>
                            <th style={{ textAlign: "right", paddingRight: 8 }}>future_low_min</th>
                            <th style={{ textAlign: "right", paddingRight: 8 }}>tp_hit_step</th>
                            <th style={{ textAlign: "right", paddingRight: 8 }}>sl_hit_step</th>
                            <th style={{ textAlign: "left", paddingRight: 8 }}>assigned_label</th>
                          </tr>
                        </thead>
                        <tbody>
                          {probeRows.slice(0, 20).map((row, idx) => (
                            <tr key={`tb-probe-${idx}`}>
                              <td style={{ textAlign: "right", paddingRight: 8 }}>{String(row.entry_price ?? "-")}</td>
                              <td style={{ textAlign: "right", paddingRight: 8 }}>{String(row.tp_price ?? "-")}</td>
                              <td style={{ textAlign: "right", paddingRight: 8 }}>{String(row.sl_price ?? "-")}</td>
                              <td style={{ textAlign: "right", paddingRight: 8 }}>{String(row.future_high_max ?? "-")}</td>
                              <td style={{ textAlign: "right", paddingRight: 8 }}>{String(row.future_low_min ?? "-")}</td>
                              <td style={{ textAlign: "right", paddingRight: 8 }}>{String(row.tp_hit_step ?? "-")}</td>
                              <td style={{ textAlign: "right", paddingRight: 8 }}>{String(row.sl_hit_step ?? "-")}</td>
                              <td style={{ textAlign: "left", paddingRight: 8 }}>{String(row.assigned_label ?? "-")}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : null}
                </div>
              );
            })()}
            {(() => {
              const fs = ((metrics?.final_metrics ?? {}) as Record<string, unknown>).feature_separability as Record<string, unknown> | undefined;
              if (!fs || typeof fs !== "object") return null;
              const rows = Object.entries(fs)
                .filter(([, v]) => !!v && typeof v === "object")
                .map(([name, raw]) => {
                  const obj = raw as Record<string, unknown>;
                  const byLabel = (obj.by_label as Record<string, unknown>) ?? {};
                  const sep = (obj.separability as Record<string, unknown>) ?? {};
                  const up = (byLabel.up as Record<string, unknown>) ?? {};
                  const flat = (byLabel.flat as Record<string, unknown>) ?? {};
                  const down = (byLabel.down as Record<string, unknown>) ?? {};
                  const maxSep = Math.max(
                    Number(sep.up_flat ?? 0) || 0,
                    Number(sep.up_down ?? 0) || 0,
                    Number(sep.flat_down ?? 0) || 0,
                  );
                  return {
                    name,
                    upMean: Number(up.mean ?? NaN),
                    flatMean: Number(flat.mean ?? NaN),
                    downMean: Number(down.mean ?? NaN),
                    upDown: Number(sep.up_down ?? NaN),
                    upFlat: Number(sep.up_flat ?? NaN),
                    flatDown: Number(sep.flat_down ?? NaN),
                    corr: Number(obj.corr_with_future_return ?? NaN),
                    rank: Number.isFinite(maxSep) ? maxSep : 0,
                  };
                })
                .sort((a, b) => b.rank - a.rank);
              if (!rows.length) return null;
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Feature Separability</div>
                  <table style={{ borderCollapse: "collapse", width: 760 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", paddingRight: 8 }}>feature</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>up mean</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>flat mean</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>down mean</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>sep U-D</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>sep U-F</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>sep F-D</th>
                        <th style={{ textAlign: "right", paddingRight: 8 }}>corr(ret)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => (
                        <tr key={`fs-${r.name}`}>
                          <td style={{ paddingRight: 8 }}>{r.name}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number.isFinite(r.upMean) ? fmt(r.upMean, 4) : "-"}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number.isFinite(r.flatMean) ? fmt(r.flatMean, 4) : "-"}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number.isFinite(r.downMean) ? fmt(r.downMean, 4) : "-"}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number.isFinite(r.upDown) ? fmt(r.upDown, 3) : "-"}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number.isFinite(r.upFlat) ? fmt(r.upFlat, 3) : "-"}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number.isFinite(r.flatDown) ? fmt(r.flatDown, 3) : "-"}</td>
                          <td style={{ textAlign: "right", paddingRight: 8 }}>{Number.isFinite(r.corr) ? fmt(r.corr, 3) : "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
            {(() => {
              const fm = (metrics?.final_metrics ?? {}) as Record<string, unknown>;
              const spacing = (fm.triple_barrier_step_spacing_stats as Record<string, unknown>) ?? {};
              const rows = Array.isArray(fm.triple_barrier_future_path_probe_rows)
                ? (fm.triple_barrier_future_path_probe_rows as Array<Record<string, unknown>>)
                : [];
              if (!Object.keys(spacing).length && !rows.length) return null;
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Triple-Barrier Time Spacing</div>
                  <div className="kv"><span>median_step_delta_ms</span><span>{fmt(Number(spacing.median_step_delta_ms ?? 0), 3)}</span></div>
                  <div className="kv"><span>min_step_delta_ms</span><span>{fmt(Number(spacing.min_step_delta_ms ?? 0), 3)}</span></div>
                  <div className="kv"><span>max_step_delta_ms</span><span>{fmt(Number(spacing.max_step_delta_ms ?? 0), 3)}</span></div>
                  <div className="kv"><span>estimated_timeout_seconds</span><span>{fmt(Number(fm.estimated_timeout_seconds ?? spacing.estimated_timeout_seconds ?? 0), 3)}</span></div>
                  <div className="kv"><span>non_monotonic_timestamp_count</span><span>{Number(spacing.non_monotonic_timestamp_count ?? 0)}</span></div>
                  <div className="kv"><span>contiguous_within_tolerance</span><span>{Boolean(spacing.contiguous_within_tolerance) ? "yes" : "no"}</span></div>
                  {rows.slice(0, 5).map((row, idx) => (
                    <div key={`tb-path-${idx}`} style={{ marginTop: 6, fontSize: 11, opacity: 0.9 }}>
                      <div>entry_price: {String(row.entry_price ?? "-")}</div>
                      <div>future_prices_0_20: {JSON.stringify(row.future_prices_0_20 ?? [])}</div>
                      <div>future_timestamps_0_20: {JSON.stringify(row.future_timestamps_0_20 ?? [])}</div>
                      <div>time_delta_ms_0_20: {JSON.stringify(row.time_delta_ms_0_20 ?? [])}</div>
                    </div>
                  ))}
                </div>
              );
            })()}
            {(() => {
              const fm = (metrics?.final_metrics ?? {}) as Record<string, unknown>;
              if (!("label_price_rejected_rate" in fm) && !("label_price_quality_guard_triggered" in fm)) return null;
              return (
                <div style={{ marginTop: 8 }}>
                  <div className="small-title">Label-Price Quality Guard</div>
                  <div className="kv"><span>source</span><span>{String(fm.label_price_source_used ?? "mid_price")}</span></div>
                  <div className="kv"><span>rejected rate</span><span>{(Number(fm.label_price_rejected_rate ?? 0) * 100).toFixed(4)}%</span></div>
                  <div className="kv"><span>rejected count</span><span>{Number(fm.label_price_rejected_count ?? 0)}</span></div>
                  <div className="kv"><span>bookticker updates</span><span>{Number(fm.bookticker_updates_total ?? 0)}</span></div>
                  <div className="kv"><span>threshold</span><span>{(Number(fm.max_label_price_rejected_rate_used ?? 0.005) * 100).toFixed(4)}%</span></div>
                  <div className="kv"><span>guard triggered</span><span>{Boolean(fm.label_price_quality_guard_triggered) ? "yes" : "no"}</span></div>
                </div>
              );
            })()}
          </div>
        ) : null}
        {replaySessionId && replayStep ? (
          <div className="kv">
            <span>Timeline</span>
            <span style={{ width: 420 }}>
              <input
                type="range"
                min={0}
                max={Math.max(0, (replayStep.frames_total ?? 1) - 1)}
                value={Math.max(0, timelineCursor)}
                onChange={(e) => queueSeek(Number(e.target.value), false)}
                onPointerUp={(e) => queueSeek(Number((e.target as HTMLInputElement).value), true)}
                style={{ width: "100%" }}
              />
            </span>
          </div>
        ) : null}
        {error ? <div className="pair-error">{error}</div> : null}
        {replayStep?.unavailable_fields?.length ? (
          <div className="pair-error">Unavailable in replay source: {replayStep.unavailable_fields.join(", ")}</div>
        ) : null}
      </section>

      <section className="panel" style={{ marginTop: 10 }}>
        <div className="small-title">Trace Viewer (Paged)</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            Performance mode
            <select
              value={replayPerfMode}
              onChange={(e) => setReplayPerfMode(e.target.value === "deep" ? "deep" : "fast")}
              title="Trace load mode"
            >
              <option value="fast">Fast (bounded load)</option>
              <option value="deep">Deep (larger load)</option>
            </select>
          </label>
          <button type="button" onClick={() => void loadTraceViewerRows()} disabled={!selectedRunId || traceViewerLoading}>
            {traceViewerLoading ? "Loading..." : "Load Full Trace"}
          </button>
          <button type="button" onClick={exportTraceViewerRows} disabled={!traceViewerRows.length}>
            Export
          </button>
          <select
            value={traceExportFormat}
            onChange={(e) => setTraceExportFormat(e.target.value === "txt" ? "txt" : "excel")}
            title="Export format"
          >
            <option value="excel">Excel file (.csv)</option>
            <option value="txt">Text file (.txt)</option>
          </select>
          <select
            value={traceExportScope}
            onChange={(e) => setTraceExportScope((e.target.value as "full" | "current_page" | "range") || "full")}
            title="Export scope"
          >
            <option value="full">Full trace</option>
            <option value="current_page">Current page</option>
            <option value="range">Page range (from-to)</option>
          </select>
          {traceExportScope === "range" ? (
            <>
              <input
                type="number"
                min={1}
                step={1}
                value={traceExportFromPage}
                onChange={(e) => setTraceExportFromPage(Math.max(1, Math.floor(Number(e.target.value) || 1)))}
                title="From page"
                style={{ width: 80 }}
              />
              <span>to</span>
              <input
                type="number"
                min={1}
                step={1}
                value={traceExportToPage}
                onChange={(e) => setTraceExportToPage(Math.max(1, Math.floor(Number(e.target.value) || 1)))}
                title="To page"
                style={{ width: 80 }}
              />
            </>
          ) : null}
          <label style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <input
              type="checkbox"
              checked={traceExportIncludeSummary}
              onChange={(e) => setTraceExportIncludeSummary(Boolean(e.target.checked))}
            />
            include epoch summary
          </label>
          <span>{traceViewerRows.length.toLocaleString()} rows loaded</span>
          <button
            type="button"
            onClick={() => setTraceViewerPage((p) => Math.max(0, p - 1))}
            disabled={traceViewerPage <= 0}
          >
            Prev
          </button>
          <button
            type="button"
            onClick={() => setTraceViewerPage((p) => (p + 1) * TRACE_VIEWER_PAGE_SIZE < traceViewerRows.length ? p + 1 : p)}
            disabled={(traceViewerPage + 1) * TRACE_VIEWER_PAGE_SIZE >= traceViewerRows.length}
          >
            Next
          </button>
          <span>
            page {traceViewerRows.length ? traceViewerPage + 1 : 0}/{Math.max(1, Math.ceil(traceViewerRows.length / TRACE_VIEWER_PAGE_SIZE))}
          </span>
        </div>
        {traceViewerError ? <div className="pair-error">{traceViewerError}</div> : null}
        {traceViewerRows.length > 0 ? (
          <div style={{ marginTop: 8, maxHeight: 320, overflow: "auto", border: "1px solid #1f3a5d", borderRadius: 6 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>idx</th>
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>action</th>
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>actual</th>
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>current price</th>
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>L/H/S</th>
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>pred%</th>
                  <th style={{ textAlign: "left", padding: "6px 8px" }}>actual%</th>
                </tr>
              </thead>
              <tbody>
                {traceViewerRows
                  .slice(traceViewerPage * TRACE_VIEWER_PAGE_SIZE, (traceViewerPage + 1) * TRACE_VIEWER_PAGE_SIZE)
                  .map((row, i) => {
                    const idx = traceViewerPage * TRACE_VIEWER_PAGE_SIZE + i;
                    const price = Number(row.price_at_prediction ?? NaN);
                    const priceKey = Number.isFinite(price) && price > 0 ? price.toFixed(tracePriceDecimals) : "";
                    const counts = priceKey ? traceActionCountsByPrice.get(priceKey) : null;
                    return (
                      <tr key={`trace-row-${idx}`}>
                        <td style={{ padding: "4px 8px" }}>{idx}</td>
                        <td style={{ padding: "4px 8px" }}>{String(row.action ?? "-")}</td>
                        <td style={{ padding: "4px 8px" }}>{String(row.actual_label ?? "-")}</td>
                        <td style={{ padding: "4px 8px" }}>{priceKey || "-"}</td>
                        <td style={{ padding: "4px 8px" }}>{counts ? `${counts.l}/${counts.h}/${counts.s}` : "-"}</td>
                        <td style={{ padding: "4px 8px" }}>{fmt(row.predicted_magnitude_pct ?? row.magnitude_pct, 4)}</td>
                        <td style={{ padding: "4px 8px" }}>{fmt(row.actual_future_return_pct, 4)}</td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>

      <TerminalPage tabId={tabId} expectedSymbol={symbol} streamOverride={replayTerminalState} />
      {fullscreenActive && replaySessionId && replayStep && fullscreenHost
        ? createPortal(
            <div
              style={{
                position: "fixed",
                left: 0,
                right: 0,
                bottom: 10,
                zIndex: 3000,
                display: "flex",
                justifyContent: "center",
                pointerEvents: "none",
              }}
            >
              <div
                style={{
                  width: "min(980px, calc(100vw - 40px))",
                  background: "rgba(7, 17, 33, 0.92)",
                  border: "1px solid rgba(130, 170, 220, 0.3)",
                  borderRadius: 10,
                  padding: "8px 12px",
                  pointerEvents: "auto",
                  backdropFilter: "blur(3px)",
                }}
              >
                <input
                  type="range"
                  min={0}
                  max={Math.max(0, (replayStep.frames_total ?? 1) - 1)}
                  value={Math.max(0, timelineCursor)}
                  onChange={(e) => queueSeek(Number(e.target.value), false)}
                  onPointerUp={(e) => queueSeek(Number((e.target as HTMLInputElement).value), true)}
                  style={{ width: "100%" }}
                />
              </div>
            </div>,
            fullscreenHost,
          )
        : null}

      <section className="panel" style={{ marginTop: 10 }}>
        <div className="small-title">Training Viewer</div>
        {liveBatchText ? <div className="kv"><span>Live batch</span><span>{liveBatchText}</span></div> : null}
        <div className="kv"><span>Epochs</span><span>{epochs.length}</span></div>
        {latestEpoch ? (
          <div className="kv"><span>Latest</span><span>epoch {fmt(latestEpoch.epoch, 0)} | val_loss {fmt(latestEpoch.val_loss, 5)} | macro_f1 {fmt(latestEpoch.val_macro_f1, 5)}</span></div>
        ) : null}
        <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
          <div>
            <div className="small-title">Val Loss</div>
            <svg width="420" height="90" viewBox="0 0 420 90">
              <path d={lossPath} stroke="#ff7a7a" strokeWidth="2" fill="none" />
              {lossSingleY !== null ? <circle cx="210" cy={lossSingleY} r="4" fill="#ff7a7a" /> : null}
            </svg>
          </div>
          <div>
            <div className="small-title">Macro F1</div>
            <svg width="420" height="90" viewBox="0 0 420 90">
              <path d={f1Path} stroke="#52e0ff" strokeWidth="2" fill="none" />
              {f1SingleY !== null ? <circle cx="210" cy={f1SingleY} r="4" fill="#52e0ff" /> : null}
            </svg>
          </div>
          <div>
            <div className="small-title">Direction Accuracy</div>
            <svg width="420" height="90" viewBox="0 0 420 90">
              <path d={accPath} stroke="#6dff9a" strokeWidth="2" fill="none" />
              {accSingleY !== null ? <circle cx="210" cy={accSingleY} r="4" fill="#6dff9a" /> : null}
            </svg>
          </div>
          <div>
            <div className="small-title">Class F1 Trends</div>
            <svg width="420" height="90" viewBox="0 0 420 90">
              <path d={classF1DownPath} stroke="#ff7a7a" strokeWidth="2" fill="none" />
              <path d={classF1FlatPath} stroke="#52e0ff" strokeWidth="2" fill="none" />
              <path d={classF1UpPath} stroke="#6dff9a" strokeWidth="2" fill="none" />
              {classF1DownSingleY !== null ? <circle cx="195" cy={classF1DownSingleY} r="3" fill="#ff7a7a" /> : null}
              {classF1FlatSingleY !== null ? <circle cx="210" cy={classF1FlatSingleY} r="3" fill="#52e0ff" /> : null}
              {classF1UpSingleY !== null ? <circle cx="225" cy={classF1UpSingleY} r="3" fill="#6dff9a" /> : null}
            </svg>
          </div>
        </div>
        <div className="small-title" style={{ marginTop: 12 }}>Final Test Metrics</div>
        <table style={{ borderCollapse: "collapse", width: 420 }}>
          <tbody>
            <tr>
              <td style={{ paddingRight: 12 }}>Samples</td>
              <td>{finalMetrics.samplesTest.toLocaleString()}</td>
            </tr>
            <tr>
              <td style={{ paddingRight: 12 }}>Direction Accuracy</td>
              <td>{(finalMetrics.directionAccuracy * 100).toFixed(2)}%</td>
            </tr>
            <tr>
              <td style={{ paddingRight: 12 }}>Macro F1</td>
              <td>{finalMetrics.macroF1.toFixed(4)}</td>
            </tr>
            <tr>
              <td style={{ paddingRight: 12 }}>Magnitude RMSE</td>
              <td>{finalMetrics.magnitudeRmse.toFixed(4)}</td>
            </tr>
            <tr>
              <td style={{ paddingRight: 12 }}>PnL Proxy</td>
              <td>{finalMetrics.pnlProxy.toFixed(4)}</td>
            </tr>
            <tr>
              <td style={{ paddingRight: 12 }}>Sharpe Proxy</td>
              <td>{finalMetrics.sharpeProxy.toFixed(4)}</td>
            </tr>
            <tr>
              <td style={{ paddingRight: 12 }}>Quality Accuracy</td>
              <td>{(finalMetrics.qualityAccuracy * 100).toFixed(2)}%</td>
            </tr>
            <tr>
              <td style={{ paddingRight: 12 }}>Best Val Loss</td>
              <td>{finalMetrics.bestValLoss.toFixed(5)}</td>
            </tr>
          </tbody>
        </table>
        <div className="small-title" style={{ marginTop: 12 }}>Final Confusion Matrix</div>
        {confusionMatrix ? (
          <table style={{ borderCollapse: "collapse", width: 420 }}>
            <thead><tr><th style={{ textAlign: "left" }}>true\pred</th><th style={{ textAlign: "left" }}>down</th><th style={{ textAlign: "left" }}>flat</th><th style={{ textAlign: "left" }}>up</th></tr></thead>
            <tbody>
              {confusionMatrix.map((row, idx) => (
                <tr key={`cm-${idx}`}>
                  <td>{idx === 0 ? "down" : idx === 1 ? "flat" : "up"}</td>
                  <td>{Number(row?.[0] ?? 0).toLocaleString()}</td>
                  <td>{Number(row?.[1] ?? 0).toLocaleString()}</td>
                  <td>{Number(row?.[2] ?? 0).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : <div className="kv"><span>Matrix</span><span>Unavailable</span></div>}
      </section>
    </>
  );
}

async function loadReplayHistoryCached(
  cacheRef: MutableRefObject<Map<string, ReplayDecisionFrame[]>>,
  replayId: string,
  cursor: number,
  framesTotal: number,
  candleLimit: number,
  split: string,
  epochIndex: number,
): Promise<ReplayDecisionFrame[]> {
  const key = `${replayId}|${split}|${epochIndex}`;
  const cached = cacheRef.current.get(key);
  if (cached && cached.length > 0) return cached;
  const loaded = await loadReplayHistoryWindow(replayId, cursor, framesTotal, candleLimit);
  cacheRef.current.set(key, loaded);
  return loaded;
}

async function loadReplayHistoryWindow(
  replayId: string,
  _cursor: number,
  framesTotal: number,
  candleLimit: number,
): Promise<ReplayDecisionFrame[]> {
  const total = Math.max(0, Number(framesTotal || 0));
  const requestedByCandles = Math.max(1200, Number(candleLimit || 240) * 20);
  const fullTraceCap = 250_000;
  const targetCount = Math.max(600, Math.min(fullTraceCap, Math.max(total, requestedByCandles)));
  // Backend endpoint enforces limit<=2000, so page through until target is reached.
  const pageSize = 2000;
  let since = 0;
  const out: ReplayDecisionFrame[] = [];
  const seen = new Set<number>();
  try {
    while (since < targetCount) {
      const limit = Math.min(pageSize, Math.max(1, targetCount - since));
      const res = await fetch(`${API_ROOT}/ml/replay/${encodeURIComponent(replayId)}/events?since=${since}&limit=${limit}`);
      const data = (await res.json()) as {
        events?: Array<{ index?: number; frame?: ReplayDecisionFrame }>;
        next_since?: number;
      };
      const events = Array.isArray(data.events) ? data.events : [];
      if (!events.length) break;
      for (const e of events) {
        const frame = (e?.frame && typeof e.frame === "object" ? e.frame : null) as ReplayDecisionFrame | null;
        if (!frame) continue;
        const idx = Number((e as { index?: number })?.index ?? NaN);
        const stepIndex = Number((frame as Record<string, unknown>).step_index ?? NaN);
        const sampleIndex = Number((frame as Record<string, unknown>).sample_index ?? NaN);
        const resolvedIndex = Number.isFinite(sampleIndex) ? sampleIndex : Number.isFinite(stepIndex) ? stepIndex : idx;
        if (Number.isFinite(resolvedIndex)) {
          const key = Math.round(resolvedIndex);
          if (seen.has(key)) continue;
          seen.add(key);
        }
        const withIndex =
          Number.isFinite(sampleIndex) || Number.isFinite(stepIndex)
            ? frame
            : ({ ...frame, step_index: Number.isFinite(idx) ? idx : 0 } as ReplayDecisionFrame);
        out.push(withIndex);
      }
      const nextSince = Number(data.next_since ?? NaN);
      if (!Number.isFinite(nextSince) || nextSince <= since) break;
      since = nextSince;
    }
    return out;
  } catch {
    return [];
  }
}

function buildReplayTerminalState(
  symbol: string,
  step: ReplayStepFrame | null,
  previous: TerminalState | null,
  baseCandles: TerminalState["klineSeries"] = [],
  replayHistoryOverride: ReplayDecisionFrame[] | null = null,
  replayGlobalCandleBubbles: ReplayBubbleStatsMap = {},
  fullChartMode: boolean = false,
): TerminalState {
  const activeFrame = step?.frame ?? null;
  const sourceMode = normalizeReplaySourceMode(step?.source_mode);
  const isMarketEventReplay = sourceMode === "market_event_replay";
  const marketSnapshotRaw =
    step?.terminal_snapshot && typeof step.terminal_snapshot === "object"
      ? (step.terminal_snapshot as Record<string, unknown>)
      : null;
  const fallbackSeries = previous?.klineSeries ?? baseCandles;
  const baseMappingSeries = baseCandles.length > 0 ? baseCandles : fallbackSeries;
  const prevDecisionLayer = (previous?.snapshot?.decision_layer ?? {}) as Record<string, unknown>;
  const prevReplayHistoryRaw = Array.isArray(prevDecisionLayer.replay_history) ? prevDecisionLayer.replay_history : [];
  const prevReplayHistory = prevReplayHistoryRaw.filter((item): item is Record<string, unknown> => !!item && typeof item === "object");
  const overrideHistory = (Array.isArray(replayHistoryOverride) ? replayHistoryOverride : [])
    .filter((item): item is ReplayDecisionFrame => !!item && typeof item === "object")
    .map((item) => ({ ...(item as Record<string, unknown>) }));
  const replayHistory: Record<string, unknown>[] = overrideHistory.length > 0 ? overrideHistory : [...prevReplayHistory];
  if (activeFrame && typeof activeFrame === "object") {
    const activeFrameObj = { ...(activeFrame as Record<string, unknown>) };
    const actionSource = activeFrameObj.action ?? activeFrameObj.predicted_action ?? activeFrameObj.predicted_label;
    activeFrameObj.action = normalizeReplayAction(actionSource);
    const plotTs = mapReplayMarkerToPlotTs(activeFrameObj, baseMappingSeries, Number(step?.frames_total ?? 0), fullChartMode);
    if (plotTs !== null) activeFrameObj.plot_ts_ms = plotTs;
    const key = Number((activeFrame as Record<string, unknown>).sample_index ?? (activeFrame as Record<string, unknown>).step_index ?? -1);
    const existingIdx = replayHistory.findIndex((row) => {
      const rowKey = Number(row.sample_index ?? row.step_index ?? -2);
      return Number.isFinite(key) && Number.isFinite(rowKey) && rowKey === key;
    });
    if (existingIdx >= 0) replayHistory[existingIdx] = activeFrameObj;
    else replayHistory.push(activeFrameObj);
  }
  const replayHistoryEnriched = replayHistory.map((row) => {
    const normalizedAction = normalizeReplayAction(row.action ?? row.predicted_action ?? row.predicted_label);
    const normalizedRow = row.action === normalizedAction ? row : { ...row, action: normalizedAction };
    if (Number.isFinite(Number(normalizedRow.plot_ts_ms ?? NaN))) return normalizedRow;
    const plotTs = mapReplayMarkerToPlotTs(normalizedRow, baseMappingSeries, Number(step?.frames_total ?? 0), fullChartMode);
    return plotTs === null ? normalizedRow : { ...normalizedRow, plot_ts_ms: plotTs };
  });
  const fullModeCandles = fullChartMode ? buildReplayCandlesFromHistory(replayHistoryEnriched, baseMappingSeries) : [];
  const mappingSeries = fullModeCandles.length > 0 ? fullModeCandles : baseMappingSeries;
  const replayHistoryForCursor = filterReplayHistoryByCursor(
    replayHistoryEnriched,
    Number(step?.cursor ?? 0),
    Number(step?.frames_total ?? 0),
  );
  const replayHistoryForBubbles = replayHistoryEnriched;
  const replayHistoryTrimmed = replayHistoryForCursor.slice(-Math.max(5000, (mappingSeries.length || 240) * 20));
  const replayCandleBubbles = buildReplayCandleBubbleStats(
    replayHistoryForBubbles,
    mappingSeries,
    Number(step?.frames_total ?? 0),
  );
  const replayCandleCoreByTs = buildReplayCandleCoreByTs(
    replayHistoryForCursor,
    mappingSeries,
    Number(step?.frames_total ?? 0),
  );
  const replayCandleStatsByTs = buildReplayCandleStatsByTs(
    replayHistoryForBubbles,
    mappingSeries,
    Number(step?.frames_total ?? 0),
  );
  const klineSeries = frameToCandles(
    activeFrame,
    fallbackSeries,
    step?.cursor ?? 0,
    step?.frames_total ?? 0,
    mappingSeries,
    fullChartMode,
  );
  const last = klineSeries[klineSeries.length - 1];
  const prev = klineSeries[klineSeries.length - 2] ?? last;
  const mark = Number(last?.close ?? 0);
  const bid = mark > 0 ? mark * 0.99995 : 0;
  const ask = mark > 0 ? mark * 1.00005 : 0;
  const spread = Math.max(0, ask - bid);
  const tradeRate = activeFrame ? Math.max(1, Math.round(Math.abs(Number(activeFrame.magnitude_pct ?? 0)) * 120)) : 0;
  const liqEvents = activeFrame ? Math.max(0, Math.round(Math.abs(Number(activeFrame.magnitude_pct ?? 0)) * 40)) : 0;
  const marketDecisionLayer =
    marketSnapshotRaw?.decision_layer && typeof marketSnapshotRaw.decision_layer === "object"
      ? (marketSnapshotRaw.decision_layer as Record<string, unknown>)
      : null;
  const marketLadderSnapshotRaw =
    marketDecisionLayer?.market_ladder_snapshot && typeof marketDecisionLayer.market_ladder_snapshot === "object"
      ? (marketDecisionLayer.market_ladder_snapshot as Record<string, unknown>)
      : null;
  const marketHeatmapColumnRaw =
    marketDecisionLayer?.market_heatmap_column && typeof marketDecisionLayer.market_heatmap_column === "object"
      ? (marketDecisionLayer.market_heatmap_column as Record<string, unknown>)
      : null;
  const marketBucketSize = Number(marketDecisionLayer?.market_heatmap_bucket_size ?? NaN);
  const marketTimeBucketMs = Number(marketDecisionLayer?.market_heatmap_time_bucket_ms ?? NaN);
  const marketMaxColumns = Number(marketDecisionLayer?.market_heatmap_max_columns ?? NaN);
  const prevDecisionLayerForHeatmap = (previous?.snapshot?.decision_layer ?? {}) as Record<string, unknown>;
  const prevReplayCursor = Number(prevDecisionLayerForHeatmap.replay_cursor ?? NaN);
  const nextReplayCursor = Number(step?.cursor ?? NaN);
  const replayCursorAdvanced =
    Number.isFinite(prevReplayCursor) &&
    Number.isFinite(nextReplayCursor) &&
    nextReplayCursor >= prevReplayCursor &&
    nextReplayCursor - prevReplayCursor <= 2;
  const ladder = marketLadderSnapshotRaw
    ? ({
        type: "ladder_snapshot",
        symbol,
        ts_ms: Number(marketLadderSnapshotRaw.ts_ms ?? Date.now()),
        current_price: Number(marketLadderSnapshotRaw.current_price ?? mark),
        bucket_size: Math.max(0.0001, Number(marketLadderSnapshotRaw.bucket_size ?? (Number.isFinite(marketBucketSize) ? marketBucketSize : 0.0001))),
        rows: Array.isArray(marketLadderSnapshotRaw.rows)
          ? (marketLadderSnapshotRaw.rows as Array<Record<string, unknown>>).map((row) => ({
              row: Number(row.row ?? 0),
              price: Number(row.price ?? 0),
              liquidity: Number(row.liquidity ?? 0),
              side: (String(row.side ?? "at") === "above" || String(row.side ?? "at") === "below" ? String(row.side) : "at") as "above" | "below" | "at",
            }))
          : [],
      } as LadderSnapshot)
    : buildLadderSnapshot(symbol, klineSeries, step?.cursor ?? 0);
  const fallbackHeatmapColumns = buildHeatmapColumns(klineSeries, ladder?.bucket_size ?? 0.0001);
  const normalizeReplayHeatmapTs = (rawTs: unknown): number => {
    const lastCandle = klineSeries[klineSeries.length - 1];
    const firstCandle = klineSeries[0];
    const fallbackTs = Number(lastCandle?.close_ts_ms ?? lastCandle?.open_ts_ms ?? Date.now());
    const n = Number(rawTs ?? NaN);
    if (!Number.isFinite(n) || n <= 0) return fallbackTs;
    // If replay sends non-epoch index-like values, snap to candle timeline.
    if (n < 946684800000) return fallbackTs;
    const minTs = Number(firstCandle?.open_ts_ms ?? fallbackTs);
    const maxTs = Number(lastCandle?.close_ts_ms ?? fallbackTs);
    // Allow a modest spillover window; clamp extreme outliers.
    const lowerBound = minTs - 6 * 60 * 60 * 1000;
    const upperBound = maxTs + 6 * 60 * 60 * 1000;
    if (n < lowerBound || n > upperBound) return fallbackTs;
    return n;
  };
  const heatmapColumns = (() => {
    if (!marketHeatmapColumnRaw) return fallbackHeatmapColumns;
    // On replay seek/scrub backwards, stale future columns can shift visible bars away from candles.
    // Keep previous columns only during small forward cursor advances.
    const prevColumns = replayCursorAdvanced && Array.isArray(previous?.heatmap?.columns) ? previous?.heatmap?.columns ?? [] : [];
    const rowsRaw = Array.isArray(marketHeatmapColumnRaw.rows) ? marketHeatmapColumnRaw.rows as Array<Record<string, unknown>> : [];
    const nextColumn: HeatmapColumn = {
      ts_ms: normalizeReplayHeatmapTs(marketHeatmapColumnRaw.ts_ms ?? Date.now()),
      center_row: Number(marketHeatmapColumnRaw.center_row ?? 0),
      row_min: Number(marketHeatmapColumnRaw.row_min ?? -20),
      row_max: Number(marketHeatmapColumnRaw.row_max ?? 20),
      rows: rowsRaw.map((row) => ({
        row: Number(row.row ?? 0),
        value: Number(row.value ?? 0),
      })),
    };
    const maxCols = Number.isFinite(marketMaxColumns) && marketMaxColumns > 0 ? Math.round(marketMaxColumns) : 14_400;
    return [...prevColumns.slice(-(maxCols - 1)), nextColumn];
  })();
  const sourceCoverage = buildReplayCoverage(symbol);
  let snapshot: TerminalState["snapshot"];
  if (marketSnapshotRaw) {
    const rawDecisionLayer =
      marketSnapshotRaw.decision_layer && typeof marketSnapshotRaw.decision_layer === "object"
        ? (marketSnapshotRaw.decision_layer as Record<string, unknown>)
        : {};
    const rawDepth =
      marketSnapshotRaw.depth_levels && typeof marketSnapshotRaw.depth_levels === "object"
        ? (marketSnapshotRaw.depth_levels as Record<string, unknown>)
        : {};
    const rawBestBid = Number(marketSnapshotRaw.best_bid ?? NaN);
    const rawBestAsk = Number(marketSnapshotRaw.best_ask ?? NaN);
    const rawMark = Number(marketSnapshotRaw.mark_price ?? NaN);
    const rawLastTrade = Number(marketSnapshotRaw.last_trade_price ?? NaN);
    const chooseReplayPrice = (candidate: number, fallback: number): number => {
      if (!Number.isFinite(candidate) || candidate <= 0) return fallback;
      // Keep replay price line aligned with replay candle context; reject far outliers.
      const gap = Math.abs(candidate - fallback) / Math.max(1e-9, fallback);
      return gap > 0.01 ? fallback : candidate;
    };
    const safeMark = chooseReplayPrice(rawMark, mark);
    const safeLastTrade = chooseReplayPrice(rawLastTrade, mark);
    const safeBid = chooseReplayPrice(rawBestBid, bid);
    const safeAsk = chooseReplayPrice(rawBestAsk, ask);
    const safeSpread = Math.max(0, safeAsk - safeBid);

    snapshot = {
      type: "terminal_snapshot",
      symbol,
      received_at: String(marketSnapshotRaw.received_at || new Date().toISOString()),
      event_count: Math.max(0, Number(marketSnapshotRaw.event_count ?? step?.cursor ?? klineSeries.length)),
      book_synced: Boolean(marketSnapshotRaw.book_synced ?? false),
      last_update_id: Math.max(0, Number(marketSnapshotRaw.last_update_id ?? step?.cursor ?? 0)),
      best_bid: safeBid,
      best_ask: safeAsk,
      spread: Number.isFinite(Number(marketSnapshotRaw.spread ?? NaN))
        ? Math.max(0, Number(marketSnapshotRaw.spread))
        : safeSpread,
      mark_price: safeMark,
      funding_rate: Number(marketSnapshotRaw.funding_rate ?? 0),
      last_trade_price: safeLastTrade,
      last_trade_qty: Number(marketSnapshotRaw.last_trade_qty ?? Math.max(0.0001, Math.abs(mark - Number(prev?.close ?? mark)) * 10)),
      last_trade_side: String(marketSnapshotRaw.last_trade_side ?? (mark >= Number(prev?.close ?? mark) ? "BUY" : "SELL")),
      last_kline_close: Number(marketSnapshotRaw.last_kline_close ?? mark),
      last_kline_interval: String(marketSnapshotRaw.last_kline_interval ?? "1m"),
      last_liquidation_side: String(marketSnapshotRaw.last_liquidation_side ?? (liqEvents > 0 ? (mark >= Number(prev?.close ?? mark) ? "LONG" : "SHORT") : "NONE")),
      last_liquidation_qty: Number(marketSnapshotRaw.last_liquidation_qty ?? (liqEvents > 0 ? liqEvents * 0.1 : 0)),
      trade_rate_10s: Number(marketSnapshotRaw.trade_rate_10s ?? tradeRate),
      liq_events_60s: Number(marketSnapshotRaw.liq_events_60s ?? liqEvents),
      sync_failures: Number(marketSnapshotRaw.sync_failures ?? 0),
      stream_counts: (marketSnapshotRaw.stream_counts && typeof marketSnapshotRaw.stream_counts === "object")
        ? (marketSnapshotRaw.stream_counts as Record<string, number>)
        : {},
      depth_levels: {
        bids: Number(rawDepth.bids ?? ladder?.rows.filter((r) => r.side === "below").length ?? 0),
        asks: Number(rawDepth.asks ?? ladder?.rows.filter((r) => r.side === "above").length ?? 0),
      },
      decision_layer: {
        ...rawDecisionLayer,
        replay_history: replayHistoryTrimmed,
        replay_candle_bubbles: replayCandleBubbles,
        replay_candle_bubbles_global: fullChartMode ? replayCandleBubbles : replayGlobalCandleBubbles,
        replay_candle_core_by_ts: replayCandleCoreByTs,
        replay_candle_stats_by_ts: replayCandleStatsByTs,
        replay_status: step?.status ?? "paused",
        replay_cursor: Number(step?.cursor ?? 0),
        replay_frames_total: Number(step?.frames_total ?? 0),
        replay_data_mode: sourceMode === "prediction_trace" ? "prediction_trace_joined_market_events" : "market_event_replay",
        replay_force_bubbles_each_candle: fullChartMode,
      },
    } as unknown as TerminalState["snapshot"];
  } else {
    snapshot = {
      type: "terminal_snapshot",
      symbol,
      received_at: new Date().toISOString(),
      event_count: Math.max(0, step?.cursor ?? klineSeries.length),
      book_synced: false,
      last_update_id: Math.max(0, step?.cursor ?? 0),
      best_bid: bid,
      best_ask: ask,
      spread,
      mark_price: mark,
      funding_rate: 0,
      last_trade_price: mark,
      last_trade_qty: Math.max(0.0001, Math.abs(mark - Number(prev?.close ?? mark)) * 10),
      last_trade_side: mark >= Number(prev?.close ?? mark) ? "BUY" : "SELL",
      last_kline_close: mark,
      last_kline_interval: "1m",
      last_liquidation_side: liqEvents > 0 ? (mark >= Number(prev?.close ?? mark) ? "LONG" : "SHORT") : "NONE",
      last_liquidation_qty: liqEvents > 0 ? liqEvents * 0.1 : 0,
      trade_rate_10s: tradeRate,
      liq_events_60s: liqEvents,
      sync_failures: 0,
      stream_counts: {},
      depth_levels: { bids: ladder?.rows.filter((r) => r.side === "below").length ?? 0, asks: ladder?.rows.filter((r) => r.side === "above").length ?? 0 },
      decision_layer: {
        replay_decision: activeFrame ?? {},
        replay_history: replayHistoryTrimmed,
        replay_candle_bubbles: replayCandleBubbles,
        replay_candle_bubbles_global: fullChartMode ? replayCandleBubbles : replayGlobalCandleBubbles,
        replay_candle_core_by_ts: replayCandleCoreByTs,
        replay_candle_stats_by_ts: replayCandleStatsByTs,
        replay_status: step?.status ?? "paused",
        replay_cursor: Number(step?.cursor ?? 0),
        replay_frames_total: Number(step?.frames_total ?? 0),
        replay_data_mode: "synthetic_from_replay_trace",
        replay_force_bubbles_each_candle: fullChartMode,
      },
    } as unknown as TerminalState["snapshot"];
  }
  return {
    events: [],
    klineSeries,
    snapshot,
    heatmap: {
      symbol,
      bucket_size: ladder?.bucket_size ?? 0.0001,
      ticks_per_bucket: 2,
      time_bucket_ms:
        Number.isFinite(marketTimeBucketMs) && marketTimeBucketMs > 0
          ? Math.round(marketTimeBucketMs)
          : 60_000,
      max_columns:
        Number.isFinite(marketMaxColumns) && marketMaxColumns > 0
          ? Math.round(marketMaxColumns)
          : 14_400,
      columns: heatmapColumns,
      ladder: ladder ?? null,
    } as unknown as TerminalState["heatmap"],
    sourceCoverage: sourceCoverage as unknown as SourceCoverageUpdate,
    symbolResolution: null,
  };
}

function buildReplayCandleCoreByTs(
  replayHistory: Array<Record<string, unknown>>,
  baseCandles: TerminalState["klineSeries"],
  framesTotal: number,
): Record<string, Record<string, unknown>> {
  const out: Record<string, Record<string, unknown>> = {};
  for (const row of replayHistory) {
    const explicitPlotTs = Number(row.plot_ts_ms ?? NaN);
    const plotTs =
      Number.isFinite(explicitPlotTs) && explicitPlotTs > 0
        ? explicitPlotTs
        : mapReplayMarkerToPlotTs(row, baseCandles, framesTotal);
    if (!Number.isFinite(Number(plotTs))) continue;
    const bucketTs = Math.floor(Number(plotTs) / 60_000) * 60_000;
    // Keep latest decision encountered for that candle bucket.
    out[String(bucketTs)] = {
      ...row,
      plot_ts_ms: bucketTs,
      action: normalizeReplayAction(row.action ?? row.predicted_action ?? row.predicted_label),
    };
  }
  return out;
}

function buildReplayCandleBubbleStats(
  replayHistory: Array<Record<string, unknown>>,
  baseCandles: TerminalState["klineSeries"],
  framesTotal: number,
): Record<
  string,
  {
    short_count: number;
    long_count: number;
    hold_count: number;
    avg_predicted_magnitude_pct: number;
  }
> {
  const acc = new Map<
    number,
    {
      short_count: number;
      long_count: number;
      hold_count: number;
      magnitude_sum: number;
      magnitude_count: number;
    }
  >();

  for (const marker of replayHistory) {
    const explicitPlotTs = Number(marker.plot_ts_ms ?? NaN);
    const plotTs = Number.isFinite(explicitPlotTs) && explicitPlotTs > 0
      ? explicitPlotTs
      : mapReplayMarkerToPlotTs(marker, baseCandles, framesTotal);
    if (!Number.isFinite(Number(plotTs))) continue;
    const bucketTs = Math.floor(Number(plotTs) / 60_000) * 60_000;
    const row = acc.get(bucketTs) ?? {
      short_count: 0,
      long_count: 0,
      hold_count: 0,
      magnitude_sum: 0,
      magnitude_count: 0,
    };
    // Bubble counts should represent model intent per candle (raw prediction),
    // not post-gate trading action which can become HOLD when blocked.
    const action = normalizeReplayAction(marker.predicted_label ?? marker.predicted_action ?? marker.action);
    if (action === "SHORT") row.short_count += 1;
    else if (action === "LONG") row.long_count += 1;
    else row.hold_count += 1;

    const magnitude = Number(marker.predicted_magnitude_pct ?? marker.magnitude_pct ?? NaN);
    if (Number.isFinite(magnitude)) {
      row.magnitude_sum += magnitude;
      row.magnitude_count += 1;
    }
    acc.set(bucketTs, row);
  }

  const out: Record<
    string,
    {
      short_count: number;
      long_count: number;
      hold_count: number;
      avg_predicted_magnitude_pct: number;
    }
  > = {};
  for (const [ts, row] of acc.entries()) {
    out[String(ts)] = {
      short_count: row.short_count,
      long_count: row.long_count,
      hold_count: row.hold_count,
      avg_predicted_magnitude_pct: row.magnitude_count > 0 ? row.magnitude_sum / row.magnitude_count : 0,
    };
  }
  return out;
}

function buildReplayCandleStatsByTs(
  replayHistory: Array<Record<string, unknown>>,
  baseCandles: TerminalState["klineSeries"],
  framesTotal: number,
): Record<
  string,
  {
    total_decisions: number;
    long_count: number;
    short_count: number;
    hold_count: number;
    avg_predicted_magnitude_pct: number;
    avg_confidence: number;
    avg_prob_up: number;
    avg_prob_down: number;
    avg_prob_flat: number;
  }
> {
  const acc = new Map<
    number,
    {
      total: number;
      long_count: number;
      short_count: number;
      hold_count: number;
      mag_sum: number;
      mag_n: number;
      conf_sum: number;
      conf_n: number;
      up_sum: number;
      up_n: number;
      down_sum: number;
      down_n: number;
      flat_sum: number;
      flat_n: number;
    }
  >();

  for (const marker of replayHistory) {
    const explicitPlotTs = Number(marker.plot_ts_ms ?? NaN);
    const plotTs = Number.isFinite(explicitPlotTs) && explicitPlotTs > 0
      ? explicitPlotTs
      : mapReplayMarkerToPlotTs(marker, baseCandles, framesTotal);
    if (!Number.isFinite(Number(plotTs))) continue;
    const bucketTs = Math.floor(Number(plotTs) / 60_000) * 60_000;
    const row = acc.get(bucketTs) ?? {
      total: 0,
      long_count: 0,
      short_count: 0,
      hold_count: 0,
      mag_sum: 0,
      mag_n: 0,
      conf_sum: 0,
      conf_n: 0,
      up_sum: 0,
      up_n: 0,
      down_sum: 0,
      down_n: 0,
      flat_sum: 0,
      flat_n: 0,
    };
    row.total += 1;
    const action = normalizeReplayAction(marker.predicted_label ?? marker.predicted_action ?? marker.action);
    if (action === "LONG") row.long_count += 1;
    else if (action === "SHORT") row.short_count += 1;
    else row.hold_count += 1;

    const mag = Number(marker.predicted_magnitude_pct ?? marker.magnitude_pct ?? NaN);
    if (Number.isFinite(mag)) {
      row.mag_sum += mag;
      row.mag_n += 1;
    }
    const conf = Number(marker.confidence ?? NaN);
    if (Number.isFinite(conf)) {
      row.conf_sum += conf;
      row.conf_n += 1;
    }
    const pup = Number(marker.prob_up ?? NaN);
    if (Number.isFinite(pup)) {
      row.up_sum += pup;
      row.up_n += 1;
    }
    const pdown = Number(marker.prob_down ?? NaN);
    if (Number.isFinite(pdown)) {
      row.down_sum += pdown;
      row.down_n += 1;
    }
    const pflat = Number(marker.prob_flat ?? NaN);
    if (Number.isFinite(pflat)) {
      row.flat_sum += pflat;
      row.flat_n += 1;
    }
    acc.set(bucketTs, row);
  }

  const out: Record<string, {
    total_decisions: number;
    long_count: number;
    short_count: number;
    hold_count: number;
    avg_predicted_magnitude_pct: number;
    avg_confidence: number;
    avg_prob_up: number;
    avg_prob_down: number;
    avg_prob_flat: number;
  }> = {};
  for (const [ts, row] of acc.entries()) {
    out[String(ts)] = {
      total_decisions: row.total,
      long_count: row.long_count,
      short_count: row.short_count,
      hold_count: row.hold_count,
      avg_predicted_magnitude_pct: row.mag_n > 0 ? row.mag_sum / row.mag_n : 0,
      avg_confidence: row.conf_n > 0 ? row.conf_sum / row.conf_n : 0,
      avg_prob_up: row.up_n > 0 ? row.up_sum / row.up_n : 0,
      avg_prob_down: row.down_n > 0 ? row.down_sum / row.down_n : 0,
      avg_prob_flat: row.flat_n > 0 ? row.flat_sum / row.flat_n : 0,
    };
  }
  return out;
}

function buildLadderSnapshot(symbol: string, series: TerminalState["klineSeries"], cursor: number): LadderSnapshot | null {
  const last = series[series.length - 1];
  if (!last) return null;
  const current = Number(last.close || 0);
  if (!Number.isFinite(current) || current <= 0) return null;
  const step = Math.max(0.000001, current * 0.0002);
  const rows = [];
  const center = 40;
  for (let i = 0; i < 81; i += 1) {
    const offset = i - center;
    const price = current + offset * step;
    const dist = Math.abs(offset);
    const liquidity = Math.max(0, (120 - dist * 2) * (1 + (Math.sin((cursor + i) * 0.2) + 1) * 0.35));
    const side: "above" | "below" | "at" = offset > 0 ? "above" : offset < 0 ? "below" : "at";
    rows.push({
      row: i,
      price,
      liquidity,
      side,
    });
  }
  return {
    type: "ladder_snapshot",
    symbol,
    ts_ms: Number(last.close_ts_ms || Date.now()),
    current_price: current,
    bucket_size: step,
    rows,
  };
}

function buildHeatmapColumns(series: TerminalState["klineSeries"], bucketSize: number): HeatmapColumn[] {
  const lookback = series.slice(-180);
  return lookback.map((k, idx) => {
    const centerRow = 220;
    const volatility = Math.max(0.2, Math.abs(Number(k.high || 0) - Number(k.low || 0)) / Math.max(0.000001, Number(k.close || 1)));
    const rows = [];
    for (let r = -22; r <= 22; r += 1) {
      const decay = Math.exp(-Math.abs(r) / 8);
      const wobble = 0.6 + 0.4 * (Math.sin((idx + r) * 0.31) + 1) * 0.5;
      const value = (decay * wobble * (1 + volatility * 8)) * 100;
      if (value > 0.8) {
        rows.push({ row: centerRow + r, value });
      }
    }
    return {
      ts_ms: Number(k.close_ts_ms || Date.now()),
      center_row: centerRow,
      row_min: centerRow - 40,
      row_max: centerRow + 40,
      rows,
    };
  });
}

function buildReplayCoverage(symbol: string): SourceCoverageUpdate {
  return {
    type: "source_coverage_update",
    symbol,
    scope: "USDT_PERPETUAL",
    total_catalog_exchanges: 1,
    supported_exchanges: 1,
    enabled_exchanges: 1,
    enabled_exchange_ids: ["replay"],
    connected_exchanges: 1,
    feature_contributors: { book: 1, trades: 1, candles: 1, mark_funding: 1, liquidations: 1 },
    exchanges: [
      {
        exchange_id: "replay",
        name: "Replay Engine",
        implemented: true,
        enabled_by_user: true,
        supports_pair: true,
        connected: true,
        reason: "ready",
        last_update_age_sec: 0,
        features: {
          book: true,
          trades: true,
          candles: true,
          mark_funding: true,
          liquidations: true,
        },
      },
    ],
    updated_at: new Date().toISOString(),
  };
}

function frameToCandles(
  frame: ReplayDecisionFrame | null,
  fallback: TerminalState["klineSeries"],
  cursor: number,
  totalFrames: number,
  baseCandles: TerminalState["klineSeries"],
  fullChartMode: boolean = false,
): TerminalState["klineSeries"] {
  const base = [...fallback];
  if (baseCandles.length > 0) {
    if (fullChartMode) return baseCandles;
    // Use a stable rolling window that follows replay cursor so slider remains meaningful
    // without blanking at mid positions.
    const total = Math.max(0, Number(totalFrames || 0));
    const clampedCursor = Math.max(0, Number(cursor || 0));
    const windowSize = Math.min(
      baseCandles.length,
      Math.max(120, Math.min(480, total > 0 ? total : 240)),
    );
    if (total <= 1 || baseCandles.length <= windowSize) return baseCandles.slice(-windowSize);
    const ratio = Math.max(0, Math.min(1, clampedCursor / Math.max(1, total - 1)));
    const endIdx = Math.max(0, Math.min(baseCandles.length - 1, Math.floor(ratio * (baseCandles.length - 1))));
    const startIdx = Math.max(0, endIdx - windowSize + 1);
    return baseCandles.slice(startIdx, endIdx + 1);
  }
  if (!frame) {
    if (base.length > 0) return base;
    const out: TerminalState["klineSeries"] = [];
    let price = 1;
    const now = Date.now();
    for (let i = 0; i < 120; i += 1) {
      const ts = now - (120 - i) * 60_000;
      out.push({ open_ts_ms: ts, close_ts_ms: ts + 59_999, open: price, high: price * 1.0005, low: price * 0.9995, close: price, interval: "1m", is_closed: true });
    }
    return out;
  }
  const prevClose = base.length > 0 ? Number(base[base.length - 1].close || 1) : 1;
  const magPct = Number(frame.predicted_magnitude_pct ?? frame.magnitude_pct ?? 0) / 100;
  const open = prevClose;
  const close = Math.max(0.000001, open * (1 + magPct));
  const high = Math.max(open, close) * 1.0008;
  const low = Math.min(open, close) * 0.9992;
  const rawTs = Number(frame.ts_ms ?? 0);
  const prevTs = base.length > 0 ? Number(base[base.length - 1].open_ts_ms || 0) : 0;
  // Prediction trace can contain sequence-like ts_ms; normalize to epoch-ms for visible charting.
  const ts =
    Number.isFinite(rawTs) && rawTs > 946684800000
      ? rawTs
      : (prevTs > 946684800000 ? prevTs + 60_000 : Date.now());
  base.push({
    open_ts_ms: ts,
    close_ts_ms: ts + 59_999,
    open,
    high,
    low,
    close,
    interval: "1m",
    is_closed: true,
  });
  return base.slice(-240);
}

function filterReplayHistoryByCursor(
  replayHistory: Array<Record<string, unknown>>,
  cursor: number,
  framesTotal: number,
): Array<Record<string, unknown>> {
  if (replayHistory.length === 0) return replayHistory;
  const clampedCursor = Math.max(0, Number(cursor || 0));
  const total = Math.max(0, Number(framesTotal || 0));
  const byIndex = replayHistory.filter((row) => {
    const rowIdx = Number(row.sample_index ?? row.step_index ?? NaN);
    if (Number.isFinite(rowIdx)) return rowIdx <= clampedCursor;
    return true;
  });
  // Some replay sources use a different index scale than cursor, which can
  // blank the chart/history until near the end. Fall back to timeline ratio.
  if (byIndex.length > 0 || total <= 1 || clampedCursor <= 0) return byIndex;
  const ratio = Math.max(0, Math.min(1, clampedCursor / Math.max(1, total - 1)));
  const keep = Math.max(1, Math.floor(replayHistory.length * ratio));
  return replayHistory.slice(0, keep);
}
