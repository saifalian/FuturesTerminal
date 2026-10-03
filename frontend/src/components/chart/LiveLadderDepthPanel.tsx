import { useEffect, useMemo, useRef, useState } from "react";
import { LadderSnapshot } from "../../types/market";

type LiveLadderDepthPanelProps = {
  ladder: LadderSnapshot | null;
  maxRowsPerSide?: number;
};

type DepthRow = {
  price: number;
  bid: number;
  ask: number;
  isCenter?: boolean;
};
type DepthDelta = { bidDelta: number; askDelta: number; at: number };

function fmtPrice(value: number): string {
  if (!Number.isFinite(value)) return "-";
  return value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 8 });
}

function fmtQty(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "-";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(1);
}

export default function LiveLadderDepthPanel({ ladder, maxRowsPerSide = 6 }: LiveLadderDepthPanelProps) {
  const [deltaByPrice, setDeltaByPrice] = useState<Record<string, DepthDelta>>({});
  const prevRowsRef = useRef<Map<number, { bid: number; ask: number }>>(new Map());

  const rows = useMemo<DepthRow[]>(() => {
    if (!ladder?.rows?.length) return [];
    const step = Math.max(0.00000001, Number(ladder.bucket_size ?? 0.0001));
    const toKey = (price: number) => Math.round(price / step);
    const fromKey = (key: number) => key * step;
    const bidByKey = new Map<number, number>();
    const askByKey = new Map<number, number>();
    for (const row of ladder.rows) {
      const key = toKey(Number(row.price));
      const qty = Math.max(0, Number(row.liquidity ?? 0));
      if (row.side === "below") bidByKey.set(key, (bidByKey.get(key) ?? 0) + qty);
      if (row.side === "above") askByKey.set(key, (askByKey.get(key) ?? 0) + qty);
    }

    const current = Number(ladder.current_price ?? NaN);
    const currentKey = toKey(current);
    const out: DepthRow[] = [];
    for (let d = maxRowsPerSide; d >= -maxRowsPerSide; d -= 1) {
      const priceKey = currentKey + d;
      out.push({
        price: fromKey(priceKey),
        bid: bidByKey.get(priceKey) ?? 0,
        ask: askByKey.get(priceKey) ?? 0,
        isCenter: d === 0,
      });
    }
    return out;
  }, [ladder, maxRowsPerSide]);

  const maxDepth = useMemo(() => {
    if (!rows.length) return 1;
    return Math.max(1, ...rows.map((r) => Math.max(r.bid, r.ask)));
  }, [rows]);

  useEffect(() => {
    const now = Date.now();
    const nextPrev = new Map<number, { bid: number; ask: number }>();
    const nextDelta: Record<string, DepthDelta> = {};

    for (const row of rows) {
      const prev = prevRowsRef.current.get(row.price);
      const bidDelta = Number.isFinite(prev?.bid as number) ? row.bid - (prev?.bid ?? 0) : 0;
      const askDelta = Number.isFinite(prev?.ask as number) ? row.ask - (prev?.ask ?? 0) : 0;
      if (prev && (Math.abs(bidDelta) > 0.000001 || Math.abs(askDelta) > 0.000001)) {
        nextDelta[String(row.price)] = { bidDelta, askDelta, at: now };
      }
      nextPrev.set(row.price, { bid: row.bid, ask: row.ask });
    }

    prevRowsRef.current = nextPrev;
    setDeltaByPrice((prev) => {
      const merged: Record<string, DepthDelta> = {};
      const cutoff = now - 700;
      for (const [k, v] of Object.entries(prev)) {
        if (v.at >= cutoff) merged[k] = v;
      }
      for (const [k, v] of Object.entries(nextDelta)) merged[k] = v;
      return merged;
    });
  }, [rows]);

  return (
    <section className="panel live-ladder-depth-panel">
      <div className="small-title">Live Ladder Depth</div>
      <div className="live-ladder-depth-head">
        <span>Price (USDT)</span>
        <span>Bid</span>
        <span>Ask</span>
      </div>
      <div className="live-ladder-depth-body">
        {rows.length === 0 ? (
          <div className="live-ladder-depth-empty">-</div>
        ) : (
          rows.map((row) => {
            const bidW = `${Math.max(0, Math.min(100, (row.bid / maxDepth) * 100))}%`;
            const askW = `${Math.max(0, Math.min(100, (row.ask / maxDepth) * 100))}%`;
            const d = deltaByPrice[String(row.price)];
            const age = d ? Date.now() - d.at : 9999;
            const active = age < 700;
            const bidConsume = active && (d?.bidDelta ?? 0) < 0;
            const askConsume = active && (d?.askDelta ?? 0) < 0;
            return (
              <div className={`live-ladder-depth-row ${row.isCenter ? "center" : ""}`} key={`depth-${row.price}`}>
                <span className={`price ${row.isCenter ? "center" : ""}`}>{fmtPrice(row.price)}</span>
                <span className={`barcell bid ${bidConsume ? "consume-flash" : ""}`}>
                  <span className="bar" style={{ width: bidW }} />
                  <strong>{fmtQty(row.bid)}</strong>
                </span>
                <span className={`barcell ask ${askConsume ? "consume-flash" : ""}`}>
                  <span className="bar" style={{ width: askW }} />
                  <strong>{fmtQty(row.ask)}</strong>
                </span>
              </div>
            );
          })
        )}
      </div>
    </section>
  );
}
