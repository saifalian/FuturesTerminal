import { type MouseEvent, type WheelEvent, useEffect, useMemo, useRef, useState } from "react";

export type BotTradeMapCandle = {
  ts_ms: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

export type BotTradeMapTrade = {
  trade_id: string;
  side: string;
  entry_price: number;
  exit_price: number;
  net_pnl: number;
  close_reason: string;
  opened_at: string;
  closed_at: string;
};

export type BotInferencePoint = {
  ts_ms: number;
  confidence: number;
  quality: number;
  action?: string;
  reason?: string;
};

type Marker = {
  x: number;
  y: number;
  label: string;
};
type CandleHover = {
  x: number;
  y: number;
  label: string;
};

type BotTradeMapProps = {
  candles: BotTradeMapCandle[];
  trades: BotTradeMapTrade[];
  inferencePoints: BotInferencePoint[];
  loading: boolean;
  error: string;
  onRefresh: () => void;
  confidenceThreshold?: number;
  qualityThreshold?: number;
  liveConfidence?: number;
  liveQuality?: number;
};

const HEIGHT = 250;
const TOP_PAD = 14;
const BOTTOM_PAD = 18;

function nearestIndex(candles: BotTradeMapCandle[], tsMs: number): number {
  if (!candles.length) return -1;
  let best = 0;
  let bestDelta = Math.abs(candles[0].ts_ms - tsMs);
  for (let i = 1; i < candles.length; i += 1) {
    const d = Math.abs(candles[i].ts_ms - tsMs);
    if (d < bestDelta) {
      best = i;
      bestDelta = d;
    }
  }
  return best;
}

export default function BotTradeMap({
  candles,
  trades,
  inferencePoints,
  loading,
  error,
  onRefresh,
  confidenceThreshold = 0.72,
  qualityThreshold = 0.72,
  liveConfidence,
  liveQuality,
}: BotTradeMapProps) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [width, setWidth] = useState(960);
  const [hover, setHover] = useState<Marker | null>(null);
  const [candleHover, setCandleHover] = useState<CandleHover | null>(null);
  const [xZoom, setXZoom] = useState(1);
  const [yZoom, setYZoom] = useState(1);
  const [xPan, setXPan] = useState(0);
  const [yPan, setYPan] = useState(0);
  const [linePrice, setLinePrice] = useState<number | null>(null);
  const [lineHover, setLineHover] = useState(false);
  const dragRef = useRef<{ dragging: boolean; x: number; y: number }>({ dragging: false, x: 0, y: 0 });
  const lineDragRef = useRef<{ dragging: boolean }>({ dragging: false });

  useEffect(() => {
    const node = wrapRef.current;
    if (!node) return;
    const resize = () => setWidth(Math.max(360, Math.floor(node.clientWidth)));
    resize();
    const observer = new ResizeObserver(resize);
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const blockNativeZoom = (event: Event) => {
      event.preventDefault();
    };
    canvas.addEventListener("wheel", blockNativeZoom, { passive: false });
    return () => {
      canvas.removeEventListener("wheel", blockNativeZoom);
    };
  }, []);

  function resetView() {
    setXZoom(1);
    setYZoom(1);
    setXPan(0);
    setYPan(0);
  }

  const sortedCandles = useMemo(
    () => [...candles].sort((a, b) => a.ts_ms - b.ts_ms).slice(-120),
    [candles]
  );

  useEffect(() => {
    setXZoom(1);
    setYZoom(1);
    setXPan(0);
    setYPan(0);
  }, [sortedCandles.length]);

  const trackedSeries = useMemo(() => {
    const confByCandle: Array<number | null> = new Array(sortedCandles.length).fill(null);
    const qualByCandle: Array<number | null> = new Array(sortedCandles.length).fill(null);
    const points = [...inferencePoints].sort((a, b) => a.ts_ms - b.ts_ms);
    for (const p of points) {
      const idx = nearestIndex(sortedCandles, Number(p.ts_ms || 0));
      if (idx < 0) continue;
      confByCandle[idx] = Math.max(0, Math.min(1, Number(p.confidence || 0)));
      qualByCandle[idx] = Math.max(0, Math.min(1, Number(p.quality || 0)));
    }
    const fallbackConf = Math.max(
      0,
      Math.min(1, Number(Number.isFinite(liveConfidence as number) ? liveConfidence : confidenceThreshold))
    );
    const fallbackQual = Math.max(
      0,
      Math.min(1, Number(Number.isFinite(liveQuality as number) ? liveQuality : qualityThreshold))
    );
    // Only fall back to threshold lines when there are zero inference points in the snapshot.
    if (!confByCandle.some((v) => v != null)) {
      for (let i = 0; i < confByCandle.length; i += 1) confByCandle[i] = fallbackConf;
    }
    if (!qualByCandle.some((v) => v != null)) {
      for (let i = 0; i < qualByCandle.length; i += 1) qualByCandle[i] = fallbackQual;
    }
    // Keep line attached to latest candle in the refreshed snapshot (not live-updated):
    // extend only the tail after the last sampled point.
    const lastConfIdx = (() => {
      for (let i = confByCandle.length - 1; i >= 0; i -= 1) {
        if (confByCandle[i] != null) return i;
      }
      return -1;
    })();
    if (lastConfIdx >= 0 && lastConfIdx < confByCandle.length - 1) {
      const v = confByCandle[lastConfIdx];
      for (let i = lastConfIdx + 1; i < confByCandle.length; i += 1) confByCandle[i] = v;
    }
    const lastQualIdx = (() => {
      for (let i = qualByCandle.length - 1; i >= 0; i -= 1) {
        if (qualByCandle[i] != null) return i;
      }
      return -1;
    })();
    if (lastQualIdx >= 0 && lastQualIdx < qualByCandle.length - 1) {
      const v = qualByCandle[lastQualIdx];
      for (let i = lastQualIdx + 1; i < qualByCandle.length; i += 1) qualByCandle[i] = v;
    }
    // Keep only the tail live from bot status (no extra fetch/load).
    if (confByCandle.length > 0) confByCandle[confByCandle.length - 1] = fallbackConf;
    if (qualByCandle.length > 0) qualByCandle[qualByCandle.length - 1] = fallbackQual;
    return { confByCandle, qualByCandle, fallbackConf, fallbackQual };
  }, [confidenceThreshold, inferencePoints, liveConfidence, liveQuality, qualityThreshold, sortedCandles]);

  const priceBounds = useMemo(() => {
    if (!sortedCandles.length) return null;
    const min = Math.min(
      ...sortedCandles.map((c) => c.low),
      ...(trades.length ? trades.map((t) => Math.min(t.entry_price, t.exit_price)) : [sortedCandles[0].low])
    );
    const max = Math.max(
      ...sortedCandles.map((c) => c.high),
      ...(trades.length ? trades.map((t) => Math.max(t.entry_price, t.exit_price)) : [sortedCandles[0].high])
    );
    return { min, max };
  }, [sortedCandles, trades]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(HEIGHT * dpr);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${HEIGHT}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    ctx.clearRect(0, 0, width, HEIGHT);
    ctx.fillStyle = "#0a1424";
    ctx.fillRect(0, 0, width, HEIGHT);
    ctx.strokeStyle = "rgba(255,255,255,0.08)";
    ctx.strokeRect(0.5, 0.5, width - 1, HEIGHT - 1);

    if (!sortedCandles.length) {
      ctx.fillStyle = "#9fb6d1";
      ctx.font = "12px sans-serif";
      ctx.fillText(loading ? "Loading candles..." : "No candle data available.", 14, 24);
      return;
    }

    const minPrice = priceBounds ? priceBounds.min : Math.min(...sortedCandles.map((c) => c.low));
    const maxPrice = priceBounds ? priceBounds.max : Math.max(...sortedCandles.map((c) => c.high));
    const span = Math.max(1e-9, maxPrice - minPrice);
    const drawHeight = HEIGHT - TOP_PAD - BOTTOM_PAD;
    const centerY = TOP_PAD + drawHeight / 2;
    const priceToYBase = (p: number) => TOP_PAD + ((maxPrice - p) / span) * drawHeight;
    const priceToY = (p: number) => (priceToYBase(p) - centerY) * yZoom + centerY + yPan;

    const step = (width / sortedCandles.length) * xZoom;
    const bodyW = Math.max(2, step * 0.6);
    const levelToPrice = (level: number) => minPrice + Math.max(0, Math.min(1, level)) * span;

    for (let i = 0; i < sortedCandles.length; i += 1) {
      const c = sortedCandles[i];
      const cx = i * step + step / 2 + xPan;
      const openY = priceToY(c.open);
      const closeY = priceToY(c.close);
      const highY = priceToY(c.high);
      const lowY = priceToY(c.low);
      const up = c.close >= c.open;
      ctx.strokeStyle = up ? "#23d18b" : "#ff6161";
      ctx.fillStyle = up ? "#23d18b" : "#ff6161";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(cx, highY);
      ctx.lineTo(cx, lowY);
      ctx.stroke();
      const y = Math.min(openY, closeY);
      const h = Math.max(1, Math.abs(closeY - openY));
      ctx.fillRect(cx - bodyW / 2, y, bodyW, h);
    }

    // Draw exactly two candle-time-tracked overlays (confidence + quality).
    const { confByCandle, qualByCandle, fallbackConf, fallbackQual } = trackedSeries;

    const drawTrackedLine = (values: Array<number | null>, color: string) => {
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.3;
      ctx.setLineDash([]);
      let started = false;
      ctx.beginPath();
      for (let i = 0; i < values.length; i += 1) {
        const v = values[i];
        if (v == null) {
          started = false;
          continue;
        }
        const x = i * step + step / 2 + xPan;
        const y = priceToY(levelToPrice(v));
        if (!started) {
          ctx.moveTo(x, y);
          started = true;
        } else {
          ctx.lineTo(x, y);
        }
      }
      ctx.stroke();
    };

    drawTrackedLine(confByCandle, "rgba(255,93,93,0.95)");
    drawTrackedLine(qualByCandle, "rgba(78,161,255,0.95)");

    const lastConf = [...confByCandle].reverse().find((v) => v != null) ?? fallbackConf;
    const lastQual = [...qualByCandle].reverse().find((v) => v != null) ?? fallbackQual;
    ctx.font = "11px sans-serif";
    ctx.fillStyle = "#ff8f8f";
    ctx.fillText(`Conf line: ${Number(lastConf).toFixed(2)}`, 10, 14);
    ctx.fillStyle = "#8dc0ff";
    ctx.fillText(`Qual line: ${Number(lastQual).toFixed(2)}`, 118, 14);

    if (linePrice != null) {
      const y = priceToY(linePrice);
      ctx.setLineDash([4, 4]);
      ctx.lineWidth = 1.2;
      ctx.strokeStyle = lineHover || lineDragRef.current.dragging ? "rgba(255,191,73,1)" : "rgba(255,191,73,0.9)";
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(width, y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "#ffbf49";
      ctx.font = "11px sans-serif";
      ctx.fillText(`H: ${linePrice.toFixed(6)}`, Math.max(8, width - 96), Math.max(12, y - 6));
    }

    ctx.fillStyle = "#6f86a2";
    ctx.font = "11px sans-serif";
    ctx.fillText(`${sortedCandles.length} candles`, 12, HEIGHT - 6);

    for (const trade of trades) {
      const openTs = new Date(trade.opened_at).getTime();
      const closeTs = new Date(trade.closed_at).getTime();
      const eIdx = nearestIndex(sortedCandles, openTs);
      const xIdx = nearestIndex(sortedCandles, closeTs);
      if (eIdx < 0 || xIdx < 0) continue;
      const xEntry = eIdx * step + step / 2 + xPan;
      const xExit = xIdx * step + step / 2 + xPan;
      const yEntry = priceToY(trade.entry_price);
      const yExit = priceToY(trade.exit_price);

      ctx.strokeStyle = "rgba(203,213,225,0.6)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(xEntry, yEntry);
      ctx.lineTo(xExit, yExit);
      ctx.stroke();

      if (String(trade.side).toUpperCase() === "LONG") {
        ctx.fillStyle = "#23d18b";
        ctx.beginPath();
        ctx.moveTo(xEntry, yEntry - 6);
        ctx.lineTo(xEntry - 5, yEntry + 4);
        ctx.lineTo(xEntry + 5, yEntry + 4);
        ctx.closePath();
        ctx.fill();
      } else {
        ctx.fillStyle = "#ff6161";
        ctx.beginPath();
        ctx.moveTo(xEntry, yEntry + 6);
        ctx.lineTo(xEntry - 5, yEntry - 4);
        ctx.lineTo(xEntry + 5, yEntry - 4);
        ctx.closePath();
        ctx.fill();
      }

      ctx.fillStyle = "#cbd5e1";
      ctx.beginPath();
      ctx.arc(xExit, yExit, 2.6, 0, Math.PI * 2);
      ctx.fill();
    }
  }, [lineHover, linePrice, loading, priceBounds, sortedCandles, trackedSeries, trades, width, xPan, yPan, xZoom, yZoom]);

  const markerPoints = useMemo(() => {
    if (!sortedCandles.length || width <= 0) return [] as Marker[];
    const minPrice = Math.min(...sortedCandles.map((c) => c.low), ...trades.map((t) => Math.min(t.entry_price, t.exit_price)));
    const maxPrice = Math.max(...sortedCandles.map((c) => c.high), ...trades.map((t) => Math.max(t.entry_price, t.exit_price)));
    const span = Math.max(1e-9, maxPrice - minPrice);
    const drawHeight = HEIGHT - TOP_PAD - BOTTOM_PAD;
    const centerY = TOP_PAD + drawHeight / 2;
    const priceToYBase = (p: number) => TOP_PAD + ((maxPrice - p) / span) * drawHeight;
    const priceToY = (p: number) => (priceToYBase(p) - centerY) * yZoom + centerY + yPan;
    const step = (width / sortedCandles.length) * xZoom;
    const out: Marker[] = [];
    for (const trade of trades) {
      const entryIdx = nearestIndex(sortedCandles, new Date(trade.opened_at).getTime());
      if (entryIdx >= 0) {
        out.push({
          x: entryIdx * step + step / 2 + xPan,
          y: priceToY(trade.entry_price),
          label: `${trade.trade_id} ${trade.side} entry=${trade.entry_price.toFixed(6)} exit=${trade.exit_price.toFixed(
            6
          )} pnl=${Number(trade.net_pnl || 0).toFixed(6)} reason=${trade.close_reason}`,
        });
      }
    }
    return out;
  }, [sortedCandles, trades, width, xPan, yPan, xZoom, yZoom]);

  function onMove(event: MouseEvent<HTMLCanvasElement>) {
    const rect = event.currentTarget.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    if (!sortedCandles.length || width <= 0 || !priceBounds) {
      setCandleHover(null);
      return;
    }
    const span = Math.max(1e-9, priceBounds.max - priceBounds.min);
    const drawHeight = HEIGHT - TOP_PAD - BOTTOM_PAD;
    const centerY = TOP_PAD + drawHeight / 2;
    const priceToYBase = (p: number) => TOP_PAD + ((priceBounds.max - p) / span) * drawHeight;
    const priceToY = (p: number) => (priceToYBase(p) - centerY) * yZoom + centerY + yPan;
    const yToPrice = (yy: number) => {
      const baseY = ((yy - yPan - centerY) / yZoom) + centerY;
      const normalized = (baseY - TOP_PAD) / drawHeight;
      return priceBounds.max - normalized * span;
    };

    if (lineDragRef.current.dragging) {
      const next = yToPrice(y);
      setLinePrice(Number.isFinite(next) ? next : linePrice);
      setLineHover(true);
      return;
    }

    if (linePrice != null) {
      const isNear = Math.abs(priceToY(linePrice) - y) <= 8;
      setLineHover(isNear);
    } else {
      setLineHover(false);
    }

    if (dragRef.current.dragging) {
      const dx = x - dragRef.current.x;
      const dy = y - dragRef.current.y;
      setXPan((prev) => prev + dx);
      setYPan((prev) => prev + dy);
      dragRef.current.x = x;
      dragRef.current.y = y;
      return;
    }
    let best: Marker | null = null;
    let bestDist = 16;
    for (const point of markerPoints) {
      const dx = point.x - x;
      const dy = point.y - y;
      const d = Math.sqrt(dx * dx + dy * dy);
      if (d < bestDist) {
        best = point;
        bestDist = d;
      }
    }
    if (best) {
      setHover(best);
      setCandleHover(null);
      return;
    }
    setHover(null);
    const step = (width / sortedCandles.length) * xZoom;
    const idxFloat = (x - xPan - step / 2) / step;
    const idx = Math.max(0, Math.min(sortedCandles.length - 1, Math.round(idxFloat)));
    const c = sortedCandles[idx];
    const conf = trackedSeries.confByCandle[idx] ?? trackedSeries.fallbackConf;
    const qual = trackedSeries.qualByCandle[idx] ?? trackedSeries.fallbackQual;
    const ts = new Date(c.ts_ms).toLocaleString();
    setCandleHover({
      x: idx * step + step / 2 + xPan,
      y: 24,
      label: `${ts} | conf=${Number(conf).toFixed(3)} qual=${Number(qual).toFixed(3)} | conf_th=${confidenceThreshold.toFixed(
        3
      )} qual_th=${qualityThreshold.toFixed(3)}`,
    });
  }

  function onMouseDown(event: MouseEvent<HTMLCanvasElement>) {
    const rect = event.currentTarget.getBoundingClientRect();
    if (linePrice != null && priceBounds) {
      const y = event.clientY - rect.top;
      const span = Math.max(1e-9, priceBounds.max - priceBounds.min);
      const drawHeight = HEIGHT - TOP_PAD - BOTTOM_PAD;
      const centerY = TOP_PAD + drawHeight / 2;
      const priceToYBase = (p: number) => TOP_PAD + ((priceBounds.max - p) / span) * drawHeight;
      const priceToY = (p: number) => (priceToYBase(p) - centerY) * yZoom + centerY + yPan;
      if (Math.abs(priceToY(linePrice) - y) <= 8) {
        lineDragRef.current.dragging = true;
        setHover(null);
        setCandleHover(null);
        return;
      }
    }
    dragRef.current.dragging = true;
    dragRef.current.x = event.clientX - rect.left;
    dragRef.current.y = event.clientY - rect.top;
    setHover(null);
    setCandleHover(null);
  }

  function onMouseUp() {
    dragRef.current.dragging = false;
    lineDragRef.current.dragging = false;
  }

  function onWheel(event: WheelEvent<HTMLCanvasElement>) {
    event.preventDefault();
    const factor = event.deltaY < 0 ? 1.08 : 0.92;
    setXZoom((prev) => Math.max(0.4, Math.min(8, prev * factor)));
    setYZoom((prev) => Math.max(0.5, Math.min(6, prev * factor)));
  }

  function zoomByFactor(factor: number) {
    setXZoom((prev) => Math.max(0.4, Math.min(8, prev * factor)));
    setYZoom((prev) => Math.max(0.5, Math.min(6, prev * factor)));
  }

  function addHorizontalLine() {
    if (!priceBounds) return;
    setLinePrice((priceBounds.min + priceBounds.max) / 2);
  }

  return (
    <section className="panel ml-subpanel">
      <div className="ml-subpanel-head">
        <div className="small-title">Bot Trade Map</div>
        <div className="ml-inline" style={{ gap: 8 }}>
          <button type="button" style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }} onClick={addHorizontalLine}>
            Add H Line
          </button>
          <button
            type="button"
            style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }}
            onClick={() => setLinePrice(null)}
            disabled={linePrice == null}
          >
            Clear H Line
          </button>
          <button type="button" style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }} onClick={resetView}>
            Reset View
          </button>
          <button type="button" style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }} onClick={onRefresh} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </div>
      {error ? <div className="coverage-error" style={{ marginBottom: 8 }}>{error}</div> : null}
      <div ref={wrapRef} style={{ position: "relative" }}>
        <div
          style={{
            position: "absolute",
            top: 8,
            right: 8,
            zIndex: 12,
            display: "flex",
            gap: 6,
          }}
        >
          <button
            type="button"
            onClick={() => zoomByFactor(1.1)}
            style={{ minHeight: 26, minWidth: 26, padding: "2px 6px", fontSize: 14, lineHeight: 1 }}
            title="Zoom In"
          >
            +
          </button>
          <button
            type="button"
            onClick={() => zoomByFactor(0.9)}
            style={{ minHeight: 26, minWidth: 26, padding: "2px 6px", fontSize: 14, lineHeight: 1 }}
            title="Zoom Out"
          >
            -
          </button>
        </div>
        <canvas
          ref={canvasRef}
          onMouseMove={onMove}
          onMouseDown={onMouseDown}
          onMouseUp={onMouseUp}
          onMouseLeave={() => {
            onMouseUp();
            setHover(null);
            setCandleHover(null);
          }}
          onWheel={onWheel}
          style={{
            cursor: lineDragRef.current.dragging ? "ns-resize" : dragRef.current.dragging ? "grabbing" : lineHover ? "ns-resize" : "grab",
            touchAction: "none",
            overscrollBehavior: "contain",
          }}
        />
        {hover ? (
          <div
            className="panel"
            style={{
              position: "absolute",
              left: Math.min(Math.max(8, hover.x + 10), Math.max(8, width - 260)),
              top: Math.min(Math.max(8, hover.y + 10), HEIGHT - 48),
              zIndex: 10,
              padding: "6px 8px",
              maxWidth: 250,
              fontSize: 11,
              lineHeight: 1.2,
            }}
          >
            {hover.label}
          </div>
        ) : null}
        {!hover && candleHover ? (
          <div
            className="panel"
            style={{
              position: "absolute",
              left: Math.min(Math.max(8, candleHover.x + 10), Math.max(8, width - 420)),
              top: Math.min(Math.max(8, candleHover.y + 10), HEIGHT - 48),
              zIndex: 10,
              padding: "6px 8px",
              maxWidth: 410,
              fontSize: 11,
              lineHeight: 1.2,
            }}
          >
            {candleHover.label}
          </div>
        ) : null}
      </div>
      <div className="ml-note" style={{ marginTop: 8 }}>
        <span style={{ color: "#ff8f8f" }}>Conf</span> and <span style={{ color: "#8dc0ff" }}>Qual</span> lines track per-candle bot confidence/quality (fallback: flat thresholds when no inference points yet). Horizontal line is price-anchored and draggable up/down. Drag to move chart, wheel/+/- to zoom, Reset View to default. Entry markers: green(up)=LONG, red(down)=SHORT. Exit marker: gray dot.
      </div>
    </section>
  );
}
