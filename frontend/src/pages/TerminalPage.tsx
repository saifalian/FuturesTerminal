import { useEffect, useMemo, useState } from "react";
import UnifiedCoinglassChart, { type ChartLiquidityProbe } from "../components/chart/UnifiedCoinglassChart";
import OrderbookDominancePanel from "../components/chart/OrderbookDominancePanel";
import PriceLadderPanel from "../components/chart/PriceLadderPanel";
import LiveLadderDepthPanel from "../components/chart/LiveLadderDepthPanel";
import AbsorptionPanel from "../components/signals/AbsorptionPanel";
import BiasMeter from "../components/signals/BiasMeter";
import CvdPanel from "../components/signals/CvdPanel";
import PositioningPanel from "../components/signals/PositioningPanel";
import SpoofingPanel from "../components/signals/SpoofingPanel";
import EmergencyCloseButton from "../components/trading/EmergencyCloseButton";
import LeftSidebar from "../components/layout/LeftSidebar";
import { useTerminalStream } from "../hooks/useTerminalStream";
import { ChartHorizontalLine } from "../types/market";
import { TerminalState } from "../app/store";

type DecisionRow = {
  label: string;
  value: string;
  tone?: "buy" | "sell";
};

type DecisionCard = {
  title: string;
  rows: DecisionRow[];
  note?: string;
  tags?: string[];
};

type ModelInputCard = {
  title: string;
  rows: Array<{ label: string; value: string; tone?: "buy" | "sell" }>;
};

type ToolMode = "live" | "replay" | "recording" | "training" | "bot";
type ToolRowId =
  | "book"
  | "candles"
  | "trades"
  | "mark_funding"
  | "liquidations"
  | "replay_candle_core"
  | "data_quality_score"
  | "weighted_exchange_controls"
  | "liquidity_event_alerts"
  | "signal_confluence_meter"
  | "execution_simulator_panel"
  | "confluence_score"
  | "long_short_checklist"
  | "entry_quality_meter"
  | "regime_detector"
  | "absorption_vs_breakout"
  | "spoof_confidence_score"
  | "mtf_alignment"
  | "playbook_tags"
  | "model_inputs_momentum"
  | "model_inputs_volatility"
  | "model_inputs_trend"
  | "model_inputs_orderflow"
  | "model_inputs_liquidity"
  | "model_inputs_context"
  | "main_chart"
  | "orderbook_dominance"
  | "live_ladder"
  | "live_ladder_depth"
  | "cvd_panel"
  | "liquidity_area"
  | "market_snapshot"
  | "bias_meter"
  | "positioning_panel"
  | "spoofing_panel"
  | "absorption_panel"
  | "safety_panel";

type ToolModeMatrix = {
  version: number;
  rows: Record<ToolRowId, Record<ToolMode, boolean>>;
};

const API_BASE = "http://127.0.0.1:8000";

const TOOL_ROW_IDS: ToolRowId[] = [
  "book",
  "candles",
  "trades",
  "mark_funding",
  "liquidations",
  "replay_candle_core",
  "data_quality_score",
  "weighted_exchange_controls",
  "liquidity_event_alerts",
  "signal_confluence_meter",
  "execution_simulator_panel",
  "confluence_score",
  "long_short_checklist",
  "entry_quality_meter",
  "regime_detector",
  "absorption_vs_breakout",
  "spoof_confidence_score",
  "mtf_alignment",
  "playbook_tags",
  "model_inputs_momentum",
  "model_inputs_volatility",
  "model_inputs_trend",
  "model_inputs_orderflow",
  "model_inputs_liquidity",
  "model_inputs_context",
  "main_chart",
  "orderbook_dominance",
  "live_ladder",
  "live_ladder_depth",
  "cvd_panel",
  "liquidity_area",
  "market_snapshot",
  "bias_meter",
  "positioning_panel",
  "spoofing_panel",
  "absorption_panel",
  "safety_panel",
];

function defaultToolModeMatrix(): ToolModeMatrix {
  const rows = {} as Record<ToolRowId, Record<ToolMode, boolean>>;
  for (const id of TOOL_ROW_IDS) {
    rows[id] = { live: true, replay: true, recording: true, training: true, bot: true };
  }
  return { version: 1, rows };
}

function normalizeToolModeMatrix(input: unknown): ToolModeMatrix {
  const base = defaultToolModeMatrix();
  if (!input || typeof input !== "object") return base;
  const rowsRaw = (input as { rows?: unknown }).rows;
  if (!rowsRaw || typeof rowsRaw !== "object") return base;
  for (const id of TOOL_ROW_IDS) {
    const rowRaw = (rowsRaw as Record<string, unknown>)[id];
    if (!rowRaw || typeof rowRaw !== "object") continue;
    for (const mode of ["live", "replay", "recording", "training", "bot"] as const) {
      if (mode in (rowRaw as Record<string, unknown>)) {
        base.rows[id][mode] = Boolean((rowRaw as Record<string, unknown>)[mode]);
      }
    }
  }
  return base;
}

function DecisionLayerCard({ card }: { card: DecisionCard }) {
  const moveDown =
    card.title === "Execution Simulator Panel" ||
    card.title === "Absorption vs Breakout";
  const narrowCard = card.title === "Signal Confluence Meter";
  return (
    <section className={`panel decision-layer-card ${moveDown ? "decision-layer-card-lowered" : ""} ${narrowCard ? "decision-layer-card-narrow" : ""}`}>
      <div className="small-title">{card.title}</div>
      {card.rows.map((row) => (
        <div className="kv" key={`${card.title}-${row.label}`}>
          <span>{row.label}</span>
          <span className={row.tone}>{row.value}</span>
        </div>
      ))}
      {card.tags && card.tags.length > 0 ? (
        <div className="decision-layer-tags">
          {card.tags.map((tag) => (
            <span key={`${card.title}-${tag}`} className="decision-layer-tag">
              {tag}
            </span>
          ))}
        </div>
      ) : null}
      {card.note ? <div className="decision-layer-note">{card.note}</div> : null}
    </section>
  );
}

function ModelInputCardView({ card }: { card: ModelInputCard }) {
  return (
    <section className="panel model-input-card">
      <div className="small-title">{card.title}</div>
      {card.rows.map((row) => (
        <div className="kv" key={`${card.title}-${row.label}`}>
          <span>{row.label}</span>
          <span className={row.tone}>{row.value}</span>
        </div>
      ))}
    </section>
  );
}

function LiquidityAreaTable({
  snapshot,
  probe,
  probeMode = false,
}: {
  snapshot: ReturnType<typeof useTerminalStream>["snapshot"];
  probe?: ChartLiquidityProbe | null;
  probeMode?: boolean;
}) {
  if (probeMode) {
    const matrix = probe?.values ?? [
      [0, 0, 0],
      [0, 0, 0],
      [0, 0, 0],
    ];
    const formatLiquidity = (value: number) => {
      if (!Number.isFinite(value) || value <= 0) return "0.0";
      if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
      if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
      return value.toFixed(1);
    };
    const formatPrice = (price: number) => {
      if (!Number.isFinite(price)) return "-";
      if (Math.abs(price) >= 1000) return price.toFixed(2);
      if (Math.abs(price) >= 1) return price.toFixed(4);
      return price.toFixed(6);
    };
    const rowPrice = (rowIndex: number) => {
      if (!probe) return NaN;
      if (rowIndex === 0) return probe.center_price + probe.bucket_size;
      if (rowIndex === 1) return probe.center_price;
      return probe.center_price - probe.bucket_size;
    };

    return (
      <table className="liquidity-area-table probe">
        <tbody>
          {matrix.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {row.map((value, colIndex) => (
                <td key={`${rowIndex}-${colIndex}`} className={rowIndex === 1 && colIndex === 1 ? "center-cell" : ""}>
                  <strong>{formatLiquidity(value)}</strong>
                </td>
              ))}
              <td className="price-cell">
                <span>Price</span>
                <strong>{formatPrice(rowPrice(rowIndex))}</strong>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }

  return (
    <table className="liquidity-area-table">
      <tbody>
        <tr>
          <td>Bid<br /><strong>{snapshot?.best_bid?.toFixed(2) ?? "-"}</strong></td>
          <td>Spread<br /><strong>{snapshot?.spread?.toFixed(2) ?? "-"}</strong></td>
          <td>Ask<br /><strong>{snapshot?.best_ask?.toFixed(2) ?? "-"}</strong></td>
        </tr>
        <tr>
          <td>Trade/s<br /><strong>{snapshot ? snapshot.trade_rate_10s.toFixed(2) : "-"}</strong></td>
          <td>Mark<br /><strong>{snapshot?.mark_price?.toFixed(2) ?? "-"}</strong></td>
          <td>Funding<br /><strong>{snapshot?.funding_rate?.toFixed(6) ?? "-"}</strong></td>
        </tr>
        <tr>
          <td>Book<br /><strong>{snapshot?.book_synced ? "Synced" : "Resync"}</strong></td>
          <td>Last<br /><strong>{snapshot?.last_trade_price?.toFixed(2) ?? "-"}</strong></td>
          <td>Liq 60s<br /><strong>{snapshot?.liq_events_60s ?? "-"}</strong></td>
        </tr>
      </tbody>
    </table>
  );
}

type TerminalPageProps = {
  tabId: string;
  expectedSymbol: string;
  streamOverride?: TerminalState | null;
};

export default function TerminalPage({
  tabId,
  expectedSymbol,
  streamOverride = null,
}: TerminalPageProps) {
  const isReplayMode = streamOverride !== null;
  const [toolModeMatrix, setToolModeMatrix] = useState<ToolModeMatrix>(() => defaultToolModeMatrix());
  const liveState = useTerminalStream(tabId, expectedSymbol, streamOverride === null);
  const { klineSeries, snapshot, heatmap, sourceCoverage } = streamOverride ?? liveState;
  const [liquidityProbeByTab, setLiquidityProbeByTab] = useState<Record<string, ChartLiquidityProbe | null>>({});
  const [replayCandleFocusByTab, setReplayCandleFocusByTab] = useState<Record<string, { ts_ms: number; locked: boolean } | null>>({});
  const [ladderOverlayByTab, setLadderOverlayByTab] = useState<Record<string, ChartHorizontalLine[]>>({});
  const liquidityProbe = liquidityProbeByTab[tabId] ?? null;
  const ladderOverlayLines = ladderOverlayByTab[tabId] ?? [];
  const ladder = heatmap?.ladder ?? null;
  const longScore = snapshot?.last_trade_side === "BUY" ? Math.min(1, 0.5 + snapshot.trade_rate_10s / 20) : 0.3;
  const shortScore = snapshot?.last_trade_side === "SELL" ? Math.min(1, 0.5 + snapshot.trade_rate_10s / 20) : 0.3;
  const confidence = snapshot ? Math.min(1, snapshot.trade_rate_10s / 10) : 0.2;
  const dangerScore = snapshot ? Math.min(1, snapshot.liq_events_60s / 20) : 0.1;
  const spread = snapshot?.spread ?? 0;
  const tradeSpeed = snapshot?.trade_rate_10s ?? 0;
  const liq60 = snapshot?.liq_events_60s ?? 0;
  const connectedExchanges = sourceCoverage?.connected_exchanges ?? 0;
  const supportedExchanges = sourceCoverage?.supported_exchanges ?? 1;
  const connectedRatio = Math.max(0, Math.min(1, connectedExchanges / Math.max(1, supportedExchanges)));
  const syncScore = snapshot?.book_synced ? 1 : 0.35;
  const spreadScore = Math.max(0, 1 - Math.min(1, spread / 0.2));
  const tradeScore = Math.min(1, tradeSpeed / 15);
  const confluenceScore = Math.round((longScore * 0.24 + shortScore * 0.18 + confidence * 0.28 + tradeScore * 0.15 + spreadScore * 0.15) * 100);
  const dataQualityScore = Math.round((connectedRatio * 0.4 + syncScore * 0.35 + spreadScore * 0.25) * 100);
  const regime =
    liq60 >= 8
      ? "Liquidation-driven"
      : tradeSpeed >= 12 && spread <= 0.05
        ? "Trend"
        : tradeSpeed <= 4
          ? "Chop"
          : "Balanced";
  const entryQuality = spread <= 0.03 && confidence >= 0.6 ? "Premium" : spread <= 0.08 ? "Neutral" : "Chase";
  const activeMode: ToolMode = isReplayMode ? "replay" : "live";

  useEffect(() => {
    let cancelled = false;
    let inFlight = false;
    async function loadToolMatrix() {
      if (inFlight) return;
      // Reduce background pressure from frequent matrix refreshes.
      // Matrix changes are rare and still refresh immediately on tab visibility.
      if (typeof document !== "undefined" && document.hidden) return;
      inFlight = true;
      try {
        const res = await fetch(`${API_BASE}/settings/tool-mode-matrix`);
        if (!res.ok) return;
        const payload = (await res.json()) as unknown;
        if (!cancelled) {
          setToolModeMatrix(normalizeToolModeMatrix(payload));
        }
      } catch {
        // keep defaults
      } finally {
        inFlight = false;
      }
    }
    void loadToolMatrix();
    const onVisible = () => {
      if (!document.hidden) void loadToolMatrix();
    };
    document.addEventListener("visibilitychange", onVisible);
    const timer = window.setInterval(() => {
      void loadToolMatrix();
    }, 30000);
    return () => {
      cancelled = true;
      document.removeEventListener("visibilitychange", onVisible);
      window.clearInterval(timer);
    };
  }, []);

  const isToolVisible = useMemo(
    () => (toolId: ToolRowId) => Boolean(toolModeMatrix.rows?.[toolId]?.[activeMode] ?? true),
    [activeMode, toolModeMatrix.rows],
  );

  const decisionLayer = (snapshot?.decision_layer ?? {}) as Record<string, unknown>;
  const section = (key: string) => {
    const value = decisionLayer[key];
    return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  };
  const readString = (bucket: Record<string, unknown>, key: string, fallback: string) => {
    const value = bucket[key];
    return value === undefined || value === null ? fallback : String(value);
  };
  const readNumber = (bucket: Record<string, unknown>, key: string, fallback: number) => {
    const value = Number(bucket[key]);
    return Number.isFinite(value) ? value : fallback;
  };
  const readBool = (bucket: Record<string, unknown>, key: string, fallback: boolean) => {
    const value = bucket[key];
    return typeof value === "boolean" ? value : fallback;
  };
  const fmtNum = (value: unknown, digits = 4) => {
    const n = Number(value);
    return Number.isFinite(n) ? n.toFixed(digits) : "-";
  };
  const fmtPctSigned = (value: unknown, digits = 3) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return "-";
    return `${(n * 100).toFixed(digits)}%`;
  };
  const toneBySign = (value: unknown): "buy" | "sell" | undefined => {
    const n = Number(value);
    if (!Number.isFinite(n) || n === 0) return undefined;
    return n > 0 ? "buy" : "sell";
  };
  const sessionLabel = (code: unknown) => {
    const n = Number(code);
    if (!Number.isFinite(n)) return "-";
    if (n < 0.5) return "Asia";
    if (n < 1.5) return "EU";
    return "US";
  };
  const replayDecision = (() => {
    const historyRaw = Array.isArray(decisionLayer.replay_history) ? (decisionLayer.replay_history as unknown[]) : [];
    const historyRows = historyRaw.filter((item): item is Record<string, unknown> => !!item && typeof item === "object");
    const fallbackFromHistory = historyRows.length
      ? historyRows.reduce<Record<string, unknown> | null>((best, row) => {
          if (!best) return row;
          const bestStep = Number(best.sample_index ?? best.step_index ?? -1);
          const rowStep = Number(row.sample_index ?? row.step_index ?? -1);
          if (Number.isFinite(rowStep) && (!Number.isFinite(bestStep) || rowStep >= bestStep)) return row;
          return best;
        }, null)
      : null;
    const fallback =
      fallbackFromHistory ??
      (decisionLayer.replay_decision && typeof decisionLayer.replay_decision === "object"
        ? (decisionLayer.replay_decision as Record<string, unknown>)
        : null);
    const focus = replayCandleFocusByTab[tabId];
    if (!focus || !Number.isFinite(Number(focus.ts_ms ?? NaN))) return fallback;
    const focusBucketTs = Math.floor(Number(focus.ts_ms) / 60_000) * 60_000;
    const coreByTsRaw =
      decisionLayer.replay_candle_core_by_ts && typeof decisionLayer.replay_candle_core_by_ts === "object"
        ? (decisionLayer.replay_candle_core_by_ts as Record<string, unknown>)
        : null;
    if (coreByTsRaw) {
      const exact = coreByTsRaw[String(focusBucketTs)];
      if (exact && typeof exact === "object") return exact as Record<string, unknown>;
    }
    let exactBest: Record<string, unknown> | null = null;
    let exactBestStep = Number.NEGATIVE_INFINITY;
    let best: Record<string, unknown> | null = null;
    let bestDist = Number.POSITIVE_INFINITY;
    for (const item of historyRaw) {
      if (!item || typeof item !== "object") continue;
      const row = item as Record<string, unknown>;
      const ts = Number(row.plot_ts_ms ?? row.ts_ms ?? NaN);
      if (!Number.isFinite(ts)) continue;
      const rowBucket = Math.floor(ts / 60_000) * 60_000;
      if (rowBucket === focusBucketTs) {
        const step = Number(row.sample_index ?? row.step_index ?? -1);
        if (step >= exactBestStep) {
          exactBestStep = step;
          exactBest = row;
        }
      }
      const d = Math.abs(ts - Number(focus.ts_ms));
      if (d < bestDist) {
        bestDist = d;
        best = row;
      }
    }
    return exactBest ?? best ?? fallback;
  })();
  const replayCoreRows: Array<{ label: string; value: string }> = replayDecision
    ? (() => {
        const focus = replayCandleFocusByTab[tabId];
        const focusBucketTs = Number.isFinite(Number(focus?.ts_ms ?? NaN))
          ? Math.floor(Number(focus?.ts_ms ?? 0) / 60_000) * 60_000
          : NaN;
        const statsByTsRaw =
          decisionLayer.replay_candle_stats_by_ts && typeof decisionLayer.replay_candle_stats_by_ts === "object"
            ? (decisionLayer.replay_candle_stats_by_ts as Record<string, unknown>)
            : null;
        const stats =
          statsByTsRaw && Number.isFinite(focusBucketTs)
            ? ((statsByTsRaw[String(focusBucketTs)] as Record<string, unknown> | undefined) ?? null)
            : null;
        return [
        {
          label: "Action",
          value: String(replayDecision.action ?? replayDecision.predicted_label ?? "HOLD").toUpperCase(),
        },
        {
          label: "Prob D/F/U",
          value: `${fmtNum(replayDecision.prob_down, 3)} / ${fmtNum(replayDecision.prob_flat, 3)} / ${fmtNum(replayDecision.prob_up, 3)}`,
        },
        {
          label: "Conf/Gap/Qual",
          value: `${fmtNum(replayDecision.confidence, 3)} / ${fmtNum(replayDecision.confidence_gap, 3)} / ${fmtNum(replayDecision.quality, 3)}`,
        },
        {
          label: "Pred Move",
          value: `${fmtNum(replayDecision.predicted_magnitude_pct ?? replayDecision.magnitude_pct, 4)}%`,
        },
        {
          label: "Actual Move",
          value: `${fmtNum(replayDecision.actual_future_return_pct, 4)}%`,
        },
        {
          label: "Correct",
          value: replayDecision.correct ? "yes" : "no",
        },
        {
          label: "Gate",
          value: replayDecision.entry_allowed
            ? "entry_allowed"
            : `blocked (${String(replayDecision.blocked_reason || "other_gate")})`,
        },
        {
          label: "Price @ Pred",
          value: fmtNum(replayDecision.price_at_prediction, 6),
        },
        {
          label: "Decisions (N)",
          value: stats ? String(Math.max(0, Number(stats.total_decisions ?? 0))) : "-",
        },
        {
          label: "Counts L/S/H",
          value: stats
            ? `${Math.max(0, Number(stats.long_count ?? 0))} / ${Math.max(0, Number(stats.short_count ?? 0))} / ${Math.max(0, Number(stats.hold_count ?? 0))}`
            : "-",
        },
        {
          label: "Avg Pred Move",
          value: stats ? fmtNum(stats.avg_predicted_magnitude_pct, 4) : "-",
        },
        {
          label: "Avg Conf",
          value: stats ? fmtNum(stats.avg_confidence, 3) : "-",
        },
      ];
      })()
    : Array.from({ length: 8 }).map((_, idx) => ({
        label: `Row ${idx + 1}`,
        value: "-",
      }));

  const displaySnapshot = useMemo(() => {
    if (!isReplayMode || !replayDecision) return snapshot;
    const base = (snapshot ?? {}) as Record<string, unknown>;
    const row = replayDecision as Record<string, unknown>;
    const preferNumeric = (key: string) => {
      const rowNum = Number(row[key]);
      if (Number.isFinite(rowNum)) return rowNum;
      const baseNum = Number(base[key]);
      if (Number.isFinite(baseNum)) return baseNum;
      return undefined;
    };
    const preferAny = (key: string) => {
      const rowValue = row[key];
      if (rowValue !== undefined && rowValue !== null && rowValue !== "") return rowValue;
      return base[key];
    };
    return {
      ...base,
      return_5s: preferNumeric("return_5s"),
      return_15s: preferNumeric("return_15s"),
      return_30s: preferNumeric("return_30s"),
      return_60s: preferNumeric("return_60s"),
      rolling_volatility_30s: preferNumeric("rolling_volatility_30s"),
      rolling_volatility_60s: preferNumeric("rolling_volatility_60s"),
      candle_range_pct: preferNumeric("candle_range_pct"),
      atr_short: preferNumeric("atr_short"),
      ema_fast_distance_pct: preferNumeric("ema_fast_distance_pct"),
      ema_slow_distance_pct: preferNumeric("ema_slow_distance_pct"),
      trend_slope_short: preferNumeric("trend_slope_short"),
      vwap_distance_pct: preferNumeric("vwap_distance_pct"),
      market_buy_volume_10s: preferNumeric("market_buy_volume_10s"),
      market_sell_volume_10s: preferNumeric("market_sell_volume_10s"),
      aggressive_buy_sell_delta: preferNumeric("aggressive_buy_sell_delta"),
      trade_rate_10s: preferNumeric("trade_rate_10s"),
      wall_strength_bid: preferNumeric("wall_strength_bid"),
      wall_strength_ask: preferNumeric("wall_strength_ask"),
      spread_change_rate: preferNumeric("spread_change_rate"),
      cancel_rate_orderbook: preferNumeric("cancel_rate_orderbook"),
      session_asia_eu_us: preferAny("session_asia_eu_us"),
      open_interest_change_pct: preferNumeric("open_interest_change_pct"),
      oi_velocity: preferNumeric("oi_velocity"),
      funding_rate_change: preferNumeric("funding_rate_change"),
    } as typeof snapshot;
  }, [isReplayMode, replayDecision, snapshot]);

  const dataQuality = section("data_quality_score");
  const weightedControls = section("weighted_exchange_controls");
  const liquidityAlerts = section("liquidity_event_alerts");
  const confluenceMeter = section("signal_confluence_meter");
  const executionSimulator = section("execution_simulator_panel");
  const confluenceScoreData = section("confluence_score");
  const checklist = section("long_short_checklist");
  const entryQualityData = section("entry_quality_meter");
  const regimeData = section("regime_detector");
  const absorptionBreakout = section("absorption_vs_breakout");
  const spoofConfidence = section("spoof_confidence_score");
  const mtfAlignment = section("mtf_alignment");
  const playbookData = section("playbook_tags");

  const decisionCards: DecisionCard[] = [
    {
      title: "Data Quality Score",
      rows: [
        {
          label: "Trust score",
          value: `${readNumber(dataQuality, "score", dataQualityScore)}/100`,
          tone: readNumber(dataQuality, "score", dataQualityScore) >= 70 ? "buy" : "sell",
        },
        {
          label: "Connected exchanges",
          value: `${readNumber(dataQuality, "connected_exchanges", connectedExchanges)}/${readNumber(dataQuality, "supported_exchanges", supportedExchanges)}`,
        },
        {
          label: "Book sync",
          value: readString(dataQuality, "book_sync_status", snapshot?.book_synced ? "Healthy" : "Resyncing"),
          tone: readString(dataQuality, "book_sync_status", snapshot?.book_synced ? "Healthy" : "Resyncing") === "Healthy" ? "buy" : "sell",
        },
      ],
      note: readString(dataQuality, "note", "Live trust based on feed coverage + sync + spread health."),
    },
    {
      title: "Weighted Exchange Controls",
      rows: [
        { label: "Mode", value: readString(weightedControls, "mode", "Auto (liq+latency)") },
        { label: "Primary", value: readString(weightedControls, "primary", "Binance") },
        { label: "Fallback", value: readString(weightedControls, "fallback", "Top live venues") },
      ],
      note: readString(weightedControls, "note", "Ready for per-exchange manual weighting."),
    },
    {
      title: "Liquidity Event Alerts",
      rows: [
        {
          label: "Wall add/remove",
          value: readString(liquidityAlerts, "wall_add_remove", liq60 > 6 ? "Triggered" : "Calm"),
          tone: readString(liquidityAlerts, "wall_add_remove", liq60 > 6 ? "Triggered" : "Calm") === "Calm" ? "buy" : "sell",
        },
        {
          label: "Spread blowout",
          value: readString(liquidityAlerts, "spread_blowout", spread > 0.2 ? "Alert" : "Normal"),
          tone: readString(liquidityAlerts, "spread_blowout", spread > 0.2 ? "Alert" : "Normal") === "Normal" ? "buy" : "sell",
        },
        { label: "Liq burst 60s", value: readString(liquidityAlerts, "liq_burst_60s", `${liq60}`) },
      ],
    },
    {
      title: "Signal Confluence Meter",
      rows: [
        {
          label: "Composite",
          value: `${readNumber(confluenceMeter, "composite_pct", confluenceScore)}%`,
          tone: readNumber(confluenceMeter, "composite_pct", confluenceScore) >= 60 ? "buy" : "sell",
        },
        { label: "Confidence", value: `${readNumber(confluenceMeter, "confidence_pct", Math.round(confidence * 100))}%` },
        {
          label: "Danger",
          value: `${readNumber(confluenceMeter, "danger_pct", Math.round(dangerScore * 100))}%`,
          tone: readNumber(confluenceMeter, "danger_pct", Math.round(dangerScore * 100)) > 50 ? "sell" : "buy",
        },
      ],
    },
    {
      title: "Execution Simulator Panel",
      rows: [
        { label: "Estimated slippage", value: readString(executionSimulator, "estimated_slippage", `${(spread * 0.6).toFixed(4)}`) },
        { label: "Fill probability", value: `${readNumber(executionSimulator, "fill_probability_pct", Math.round((tradeScore * 0.5 + spreadScore * 0.5) * 100))}%` },
        { label: "Mode", value: readString(executionSimulator, "mode", "Paper only") },
      ],
    },
    {
      title: "Confluence Score (0-100)",
      rows: [
        {
          label: "Score",
          value: readString(confluenceScoreData, "score", `${confluenceScore}`),
          tone: readNumber(confluenceScoreData, "score", confluenceScore) >= 60 ? "buy" : "sell",
        },
        { label: "Long bias", value: `${readNumber(confluenceScoreData, "long_bias_pct", Math.round(longScore * 100))}%`, tone: "buy" },
        { label: "Short bias", value: `${readNumber(confluenceScoreData, "short_bias_pct", Math.round(shortScore * 100))}%`, tone: "sell" },
      ],
    },
    {
      title: "Long / Short Checklist",
      rows: [
        { label: "Trend aligned", value: readBool(checklist, "trend_aligned", regime === "Trend") ? "Yes" : "No", tone: readBool(checklist, "trend_aligned", regime === "Trend") ? "buy" : "sell" },
        { label: "Spread healthy", value: readBool(checklist, "spread_healthy", spread <= 0.08) ? "Yes" : "No", tone: readBool(checklist, "spread_healthy", spread <= 0.08) ? "buy" : "sell" },
        { label: "Liquidity support", value: readBool(checklist, "liquidity_support", liq60 >= 2) ? "Yes" : "No", tone: readBool(checklist, "liquidity_support", liq60 >= 2) ? "buy" : "sell" },
      ],
    },
    {
      title: "Entry Quality Meter",
      rows: [
        {
          label: "Current quality",
          value: readString(entryQualityData, "current_quality", entryQuality),
          tone: readString(entryQualityData, "current_quality", entryQuality) === "Premium" ? "buy" : readString(entryQualityData, "current_quality", entryQuality) === "Chase" ? "sell" : undefined,
        },
        { label: "Spread", value: readString(entryQualityData, "spread", spread.toFixed(4)) },
        { label: "Trade speed", value: `${readNumber(entryQualityData, "trade_speed", tradeSpeed).toFixed(2)}/s` },
      ],
    },
    {
      title: "Regime Detector",
      rows: [
        { label: "Detected regime", value: readString(regimeData, "detected_regime", regime) },
        { label: "Trade speed", value: `${readNumber(regimeData, "trade_speed", tradeSpeed).toFixed(2)}/s` },
        { label: "Liq 60s", value: readString(regimeData, "liq_60s", `${liq60}`) },
      ],
    },
    {
      title: "Absorption vs Breakout",
      rows: [
        { label: "Detector", value: readString(absorptionBreakout, "detector", tradeSpeed > 10 && spread <= 0.08 ? "Breakout risk" : "Absorption bias") },
        { label: "Absorption proxy", value: readString(absorptionBreakout, "absorption_proxy", Math.max(0, tradeSpeed - spread).toFixed(2)) },
        { label: "Breakout pressure", value: `${readNumber(absorptionBreakout, "breakout_pressure_pct", Math.round(tradeScore * 100))}%` },
      ],
    },
    {
      title: "Spoof Confidence Score",
      rows: [
        { label: "Confidence", value: `${readNumber(spoofConfidence, "confidence_pct", Math.max(0, 100 - (snapshot?.sync_failures ?? 0) * 5))}%` },
        { label: "Anomaly count", value: readString(spoofConfidence, "anomaly_count", `${snapshot?.sync_failures ?? 0}`) },
        {
          label: "Depth health",
          value: readString(spoofConfidence, "depth_health", snapshot?.book_synced ? "Stable" : "Unstable"),
          tone: readString(spoofConfidence, "depth_health", snapshot?.book_synced ? "Stable" : "Unstable") === "Stable" ? "buy" : "sell",
        },
      ],
    },
    {
      title: "Multi-Timeframe Alignment",
      rows: [
        {
          label: "1m",
          value: readString(mtfAlignment, "m1", snapshot?.last_trade_side === "BUY" ? "Bullish" : "Bearish"),
          tone: readString(mtfAlignment, "m1", snapshot?.last_trade_side === "BUY" ? "Bullish" : "Bearish") === "Bullish" ? "buy" : "sell",
        },
        { label: "5m", value: readString(mtfAlignment, "m5", confidence > 0.55 ? "Aligned" : "Mixed") },
        { label: "15m", value: readString(mtfAlignment, "m15", dangerScore < 0.4 ? "Supportive" : "Risky") },
      ],
    },
    {
      title: "Playbook Tags",
      rows: [
        { label: "Active playbook", value: readString(playbookData, "active_playbook", regime === "Trend" ? "Breakout Pullback" : regime === "Liquidation-driven" ? "Sweep/Fade" : "Range Rotation") },
      ],
      tags: Array.isArray(playbookData.tags)
        ? playbookData.tags.map((item) => String(item))
        : [
            regime === "Trend" ? "trend" : "range",
            snapshot?.last_trade_side === "BUY" ? "long-pressure" : "short-pressure",
            spread <= 0.08 ? "tight-spread" : "wide-spread",
            liq60 > 4 ? "liquidation-active" : "calm-flow",
          ],
    },
  ];

  const modelInputCards: ModelInputCard[] = [
    {
      title: "Model Inputs: Momentum",
      rows: [
        { label: "Return 5s", value: fmtPctSigned(displaySnapshot?.return_5s, 3), tone: toneBySign(displaySnapshot?.return_5s) },
        { label: "Return 15s", value: fmtPctSigned(displaySnapshot?.return_15s, 3), tone: toneBySign(displaySnapshot?.return_15s) },
        { label: "Return 30s", value: fmtPctSigned(displaySnapshot?.return_30s, 3), tone: toneBySign(displaySnapshot?.return_30s) },
        { label: "Return 60s", value: fmtPctSigned(displaySnapshot?.return_60s, 3), tone: toneBySign(displaySnapshot?.return_60s) },
      ],
    },
    {
      title: "Model Inputs: Volatility",
      rows: [
        { label: "Vol 30s", value: fmtPctSigned(displaySnapshot?.rolling_volatility_30s, 3) },
        { label: "Vol 60s", value: fmtPctSigned(displaySnapshot?.rolling_volatility_60s, 3) },
        { label: "Candle range %", value: fmtPctSigned(displaySnapshot?.candle_range_pct, 3) },
        { label: "ATR short %", value: fmtPctSigned(displaySnapshot?.atr_short, 3) },
      ],
    },
    {
      title: "Model Inputs: Trend",
      rows: [
        { label: "EMA fast dist", value: fmtPctSigned(displaySnapshot?.ema_fast_distance_pct, 3), tone: toneBySign(displaySnapshot?.ema_fast_distance_pct) },
        { label: "EMA slow dist", value: fmtPctSigned(displaySnapshot?.ema_slow_distance_pct, 3), tone: toneBySign(displaySnapshot?.ema_slow_distance_pct) },
        { label: "Trend slope", value: fmtPctSigned(displaySnapshot?.trend_slope_short, 3), tone: toneBySign(displaySnapshot?.trend_slope_short) },
        { label: "VWAP dist", value: fmtPctSigned(displaySnapshot?.vwap_distance_pct, 3), tone: toneBySign(displaySnapshot?.vwap_distance_pct) },
      ],
    },
    {
      title: "Model Inputs: Orderflow",
      rows: [
        { label: "Buy vol 10s", value: fmtNum(displaySnapshot?.market_buy_volume_10s, 2) },
        { label: "Sell vol 10s", value: fmtNum(displaySnapshot?.market_sell_volume_10s, 2) },
        { label: "Aggressive delta", value: fmtPctSigned(displaySnapshot?.aggressive_buy_sell_delta, 2), tone: toneBySign(displaySnapshot?.aggressive_buy_sell_delta) },
        { label: "Trade rate 10s", value: fmtNum(displaySnapshot?.trade_rate_10s, 2) },
      ],
    },
    {
      title: "Model Inputs: Liquidity",
      rows: [
        { label: "Wall strength bid", value: fmtPctSigned(displaySnapshot?.wall_strength_bid, 2) },
        { label: "Wall strength ask", value: fmtPctSigned(displaySnapshot?.wall_strength_ask, 2) },
        { label: "Spread change", value: fmtPctSigned(displaySnapshot?.spread_change_rate, 2), tone: toneBySign(displaySnapshot?.spread_change_rate) },
        { label: "Cancel rate", value: fmtNum(displaySnapshot?.cancel_rate_orderbook, 2) },
      ],
    },
    {
      title: "Model Inputs: Context",
      rows: [
        { label: "Session", value: sessionLabel(displaySnapshot?.session_asia_eu_us) },
        { label: "OI change", value: fmtPctSigned(displaySnapshot?.open_interest_change_pct, 3), tone: toneBySign(displaySnapshot?.open_interest_change_pct) },
        { label: "OI velocity", value: fmtPctSigned(displaySnapshot?.oi_velocity, 3), tone: toneBySign(displaySnapshot?.oi_velocity) },
        { label: "Funding Δ", value: fmtNum(displaySnapshot?.funding_rate_change, 6), tone: toneBySign(displaySnapshot?.funding_rate_change) },
      ],
    },
  ];

  const decisionToolMap: Record<string, ToolRowId> = {
    "Data Quality Score": "data_quality_score",
    "Weighted Exchange Controls": "weighted_exchange_controls",
    "Liquidity Event Alerts": "liquidity_event_alerts",
    "Signal Confluence Meter": "signal_confluence_meter",
    "Execution Simulator Panel": "execution_simulator_panel",
    "Confluence Score (0-100)": "confluence_score",
    "Long / Short Checklist": "long_short_checklist",
    "Entry Quality Meter": "entry_quality_meter",
    "Regime Detector": "regime_detector",
    "Absorption vs Breakout": "absorption_vs_breakout",
    "Spoof Confidence Score": "spoof_confidence_score",
    "Multi-Timeframe Alignment": "mtf_alignment",
    "Playbook Tags": "playbook_tags",
  };
  const modelInputToolMap: Record<string, ToolRowId> = {
    "Model Inputs: Momentum": "model_inputs_momentum",
    "Model Inputs: Volatility": "model_inputs_volatility",
    "Model Inputs: Trend": "model_inputs_trend",
    "Model Inputs: Orderflow": "model_inputs_orderflow",
    "Model Inputs: Liquidity": "model_inputs_liquidity",
    "Model Inputs: Context": "model_inputs_context",
  };
  const visibleDecisionCards = decisionCards.filter((card) => {
    const toolId = decisionToolMap[card.title];
    return toolId ? isToolVisible(toolId) : true;
  });
  const visibleModelInputCards = modelInputCards.filter((card) => {
    const toolId = modelInputToolMap[card.title];
    return toolId ? isToolVisible(toolId) : true;
  });

  return (
    <div className="terminal-grid">
      <div className="sidebar-stack left-sidebar-stack">
        <LeftSidebar snapshot={snapshot} hideTradingPanels={isReplayMode} />
        {isReplayMode && isToolVisible("replay_candle_core") ? (
          <section className="panel replay-candle-core-table-panel">
            <div className="small-title">Replay Candle Core</div>
            <table className="replay-candle-core-table">
              <tbody>
                {replayCoreRows.slice(0, 12).map((row) => (
                  <tr key={row.label}>
                    <td>{row.label}</td>
                    <td>{row.value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        ) : null}
      </div>

      <div className="center-stack">
        <div className="center-chart-main">
          {isToolVisible("main_chart") ? (
            <UnifiedCoinglassChart
              klineSeries={klineSeries}
              snapshot={snapshot}
              heatmap={heatmap}
              overlayLines={ladderOverlayLines}
              drawingPersistenceKey={`${tabId}:${expectedSymbol}:${isReplayMode ? "replay" : "live"}`}
              onReplayCandleFocus={(focus) =>
                setReplayCandleFocusByTab((prev) => ({
                  ...prev,
                  [tabId]: focus,
                }))
              }
              onLiquidityProbe={(probe) =>
                setLiquidityProbeByTab((prev) => ({
                  ...prev,
                  [tabId]: probe,
                }))
              }
            />
          ) : (
            <section className="panel"><div className="ml-note">Main chart disabled in Manage Tools.</div></section>
          )}
        </div>
        <div className={`center-side-stack ${isReplayMode ? "replay-mode" : ""}`}>
          {isToolVisible("orderbook_dominance") ? (
            <div className={`center-dominance ${isReplayMode ? "replay-mode" : ""}`}>
              <OrderbookDominancePanel ladder={ladder} />
            </div>
          ) : null}
          {isReplayMode ? (
            <div className="replay-ladder-cvd-stack">
              {isToolVisible("live_ladder") ? <div className="center-ladder replay-mode">
                <PriceLadderPanel
                  ladder={ladder}
                  tabId={tabId}
                  onOverlayLinesChange={(lines) =>
                    setLadderOverlayByTab((prev) => ({
                      ...prev,
                      [tabId]: lines,
                    }))
                  }
                />
              </div> : null}
            </div>
          ) : (
            isToolVisible("live_ladder") ? <div className="center-ladder">
              <PriceLadderPanel
                ladder={ladder}
                tabId={tabId}
                onOverlayLinesChange={(lines) =>
                  setLadderOverlayByTab((prev) => ({
                    ...prev,
                    [tabId]: lines,
                  }))
                }
              />
            </div> : null
          )}
          {!isReplayMode && isToolVisible("live_ladder_depth") ? <LiveLadderDepthPanel ladder={ladder} /> : null}
        </div>
      </div>

      <div className={`bottom-strip ${isReplayMode ? "replay-mode" : ""}`}>
        {!isReplayMode && isToolVisible("cvd_panel") ? <CvdPanel snapshot={snapshot} /> : null}
        {isReplayMode ? <div className="replay-bottom-spacer panel" aria-hidden="true" /> : null}
        {isToolVisible("liquidity_area") ? <section className="panel absorption-context-panel">
          <div className="small-title">Liquidity Area</div>
          <LiquidityAreaTable snapshot={snapshot} probe={liquidityProbe} probeMode />
          <div className="liquidity-probe-hint">Top/Mid/Bottom = above/current/below pointer row</div>
          <div className="liquidity-probe-hint">Left/Mid/Right = previous/current/next chart column, last column = row price</div>
        </section> : null}
        {isToolVisible("market_snapshot") ? <section className="panel absorption-context-panel">
          <div className="small-title">Market Snapshot</div>
          <LiquidityAreaTable snapshot={snapshot} />
        </section> : null}
        {isReplayMode && isToolVisible("cvd_panel") ? (
          <div className="replay-cvd-below-market">
            <CvdPanel snapshot={snapshot} />
          </div>
        ) : null}
      </div>

      <div className="secondary-strip">
        <div className="secondary-left-stack">
          {isToolVisible("bias_meter") ? <BiasMeter
            bias={{
              long_score: longScore,
              short_score: shortScore,
              confidence,
              danger_score: dangerScore,
              reason_tags: snapshot?.book_synced ? ["depth_synced"] : ["depth_resync"],
            }}
          /> : null}
          {isToolVisible("positioning_panel") ? <PositioningPanel snapshot={snapshot} /> : null}
          {isToolVisible("spoofing_panel") ? <SpoofingPanel snapshot={snapshot} /> : null}
        </div>
        {isToolVisible("absorption_panel") ? <AbsorptionPanel snapshot={snapshot} /> : null}
        {isToolVisible("safety_panel") ? <section className="panel">
          <div className="small-title">Safety</div>
          <EmergencyCloseButton />
        </section> : null}
      </div>

      <div className="decision-layer-strip">
        {visibleDecisionCards.map((card) => (
          <DecisionLayerCard key={card.title} card={card} />
        ))}
      </div>
      <div className="model-input-strip">
        {visibleModelInputCards.map((card) => (
          <ModelInputCardView key={card.title} card={card} />
        ))}
      </div>
    </div>
  );
}

