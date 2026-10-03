import { useMemo, useState } from "react";
import { LadderRow, LadderSnapshot } from "../../types/market";

type OrderbookDominancePanelProps = {
  ladder: LadderSnapshot | null;
};

type AnchorMode = "current" | "custom";

type DominanceSettings = {
  anchorMode: AnchorMode;
  customAnchorPrice: string;
  rowsAbove: number;
  rowsBelow: number;
};

type DominanceMetrics = {
  buyLiquidity: number;
  sellLiquidity: number;
  buyPct: number;
  sellPct: number;
  anchorPriceUsed: number;
  anchorRequested: number;
  outOfWindow: boolean;
  status: string;
  dominanceLabel: "Buy Dominant" | "Sell Dominant" | "Balanced";
};

function clampInt(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  const rounded = Math.round(value);
  return Math.max(min, Math.min(max, rounded));
}

function formatLiquidity(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0.0";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(1);
}

function formatPrice(price: number): string {
  if (!Number.isFinite(price)) return "-";
  if (Math.abs(price) >= 1000) return price.toFixed(2);
  if (Math.abs(price) >= 1) return price.toFixed(4);
  return price.toFixed(6);
}

function nearestRowIndex(rows: LadderRow[], anchor: number): number {
  let bestIndex = 0;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (let i = 0; i < rows.length; i += 1) {
    const distance = Math.abs(rows[i].price - anchor);
    if (distance < bestDistance) {
      bestDistance = distance;
      bestIndex = i;
    }
  }
  return bestIndex;
}

export default function OrderbookDominancePanel({ ladder }: OrderbookDominancePanelProps) {
  const [showSettings, setShowSettings] = useState(false);
  const [settings, setSettings] = useState<DominanceSettings>({
    anchorMode: "current",
    customAnchorPrice: "",
    rowsAbove: 12,
    rowsBelow: 12,
  });

  const metrics = useMemo<DominanceMetrics>(() => {
    if (!ladder || !ladder.rows.length) {
      return {
        buyLiquidity: 0,
        sellLiquidity: 0,
        buyPct: 0,
        sellPct: 0,
        anchorPriceUsed: NaN,
        anchorRequested: NaN,
        outOfWindow: false,
        status: "Waiting for ladder data",
        dominanceLabel: "Balanced",
      };
    }

    const rows = ladder.rows;
    const anchorRequested =
      settings.anchorMode === "custom" && Number.isFinite(Number(settings.customAnchorPrice))
        ? Number(settings.customAnchorPrice)
        : ladder.current_price;
    const atIndex = rows.findIndex((row) => row.side === "at");
    const anchorIndex =
      settings.anchorMode === "current" && atIndex >= 0 ? atIndex : nearestRowIndex(rows, anchorRequested);
    const anchorPriceUsed = rows[anchorIndex]?.price ?? ladder.current_price;

    const maxPrice = Math.max(...rows.map((row) => row.price));
    const minPrice = Math.min(...rows.map((row) => row.price));
    const outOfWindow = settings.anchorMode === "custom" && (anchorRequested > maxPrice || anchorRequested < minPrice);

    const topStart = Math.max(0, anchorIndex - settings.rowsAbove);
    const sellRows = rows.slice(topStart, anchorIndex);
    const buyRows = rows.slice(anchorIndex + 1, anchorIndex + 1 + settings.rowsBelow);

    const sellLiquidity = sellRows.reduce((sum, row) => sum + Math.max(0, row.liquidity), 0);
    const buyLiquidity = buyRows.reduce((sum, row) => sum + Math.max(0, row.liquidity), 0);
    const total = sellLiquidity + buyLiquidity;
    const sellPct = total > 0 ? (sellLiquidity / total) * 100 : 0;
    const buyPct = total > 0 ? (buyLiquidity / total) * 100 : 0;

    let dominanceLabel: DominanceMetrics["dominanceLabel"] = "Balanced";
    if (Math.abs(sellPct - buyPct) > 4) {
      dominanceLabel = buyPct > sellPct ? "Buy Dominant" : "Sell Dominant";
    }

    const status = outOfWindow
      ? "Anchor outside ladder (nearest row)"
      : settings.anchorMode === "custom"
        ? "Custom anchor"
        : "Current anchor";

    return {
      buyLiquidity,
      sellLiquidity,
      buyPct,
      sellPct,
      anchorPriceUsed,
      anchorRequested,
      outOfWindow,
      status,
      dominanceLabel,
    };
  }, [ladder, settings.anchorMode, settings.customAnchorPrice, settings.rowsAbove, settings.rowsBelow]);

  const onRowsAboveChange = (value: number) => {
    setSettings((prev) => ({ ...prev, rowsAbove: clampInt(value, 1, 100) }));
  };

  const onRowsBelowChange = (value: number) => {
    setSettings((prev) => ({ ...prev, rowsBelow: clampInt(value, 1, 100) }));
  };

  const onAnchorModeChange = (nextMode: AnchorMode) => {
    setSettings((prev) => ({
      ...prev,
      anchorMode: nextMode,
      customAnchorPrice:
        nextMode === "custom"
          ? prev.customAnchorPrice || (Number.isFinite(metrics.anchorPriceUsed) ? metrics.anchorPriceUsed.toFixed(4) : "")
          : prev.customAnchorPrice,
    }));
  };

  return (
    <aside className="orderbook-dominance-panel">
      <div className="orderbook-dominance-header">
        <div className="small-title">Dominance</div>
        <button
          className="dominance-settings-btn"
          type="button"
          onClick={() => setShowSettings((v) => !v)}
          aria-label="Dominance settings"
          title="Dominance settings"
        >
          Cfg
        </button>
      </div>

      {showSettings && (
        <div className="dominance-settings-popover">
          <label className="dominance-setting-row">
            <span>Anchor</span>
            <select
              value={settings.anchorMode}
              onChange={(event) => onAnchorModeChange(event.target.value === "custom" ? "custom" : "current")}
            >
              <option value="current">Current</option>
              <option value="custom">Custom</option>
            </select>
          </label>

          {settings.anchorMode === "custom" && (
            <label className="dominance-setting-row">
              <span>Price</span>
              <input
                type="number"
                step="0.0001"
                value={settings.customAnchorPrice}
                onChange={(event) => setSettings((prev) => ({ ...prev, customAnchorPrice: event.target.value }))}
                placeholder="Set anchor price"
              />
            </label>
          )}

          <label className="dominance-setting-row">
            <span>Rows Above</span>
            <input
              type="number"
              min={1}
              max={100}
              value={settings.rowsAbove}
              onChange={(event) => onRowsAboveChange(Number(event.target.value))}
            />
          </label>

          <label className="dominance-setting-row">
            <span>Rows Below</span>
            <input
              type="number"
              min={1}
              max={100}
              value={settings.rowsBelow}
              onChange={(event) => onRowsBelowChange(Number(event.target.value))}
            />
          </label>
        </div>
      )}

      <div className="dominance-meta">
        <div className="dominance-meta-label sell">{metrics.sellPct.toFixed(1)}% Sell</div>
        <div className="dominance-meta-label buy">{metrics.buyPct.toFixed(1)}% Buy</div>
      </div>

      <div className="dominance-meter">
        <div className="dominance-meter-sell" style={{ height: `${metrics.sellPct}%` }} />
        <div className="dominance-meter-mid" />
        <div className="dominance-meter-buy" style={{ height: `${metrics.buyPct}%` }} />
      </div>

      <div className="dominance-amounts">
        <div className="dominance-amount sell">
          <span>Sell Liq</span>
          <strong>{formatLiquidity(metrics.sellLiquidity)}</strong>
        </div>
        <div className="dominance-amount buy">
          <span>Buy Liq</span>
          <strong>{formatLiquidity(metrics.buyLiquidity)}</strong>
        </div>
      </div>

      <div className="dominance-anchor-line">
        Anchor: {formatPrice(metrics.anchorPriceUsed)}
        {settings.anchorMode === "custom" && Number.isFinite(metrics.anchorRequested)
          ? ` (req ${formatPrice(metrics.anchorRequested)})`
          : ""}
      </div>
      <div className={`dominance-status ${metrics.outOfWindow ? "warn" : ""}`}>
        {metrics.dominanceLabel} - {metrics.status}
      </div>
    </aside>
  );
}
