import { useEffect, useMemo, useRef, useState } from "react";
import { KlinePoint, TerminalSnapshot } from "../../types/market";

type CandlesOnlyChartProps = {
  klineSeries: KlinePoint[];
  snapshot: TerminalSnapshot | null;
};

type Candle = {
  ts_ms: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

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

export default function CandlesOnlyChart({ klineSeries, snapshot }: CandlesOnlyChartProps) {
  const timeframeOptions = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"];
  const [timeframe, setTimeframe] = useState("1m");
  const [size, setSize] = useState({ width: 900, height: 520 });
  const containerRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  const timeframeMs = useMemo(() => timeframeToMs(timeframe), [timeframe]);
  const candles = useMemo(() => aggregateCandles(klineSeries, timeframeMs), [klineSeries, timeframeMs]);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect) return;
      setSize({
        width: Math.max(320, Math.floor(rect.width)),
        height: Math.max(260, Math.floor(rect.height)),
      });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

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

    const axisWidth = 86;
    const chartWidth = Math.max(120, size.width - axisWidth);
    const chartHeight = size.height;
    const padding = 8;
    const minSlotWidth = 4;
    const maxSlotWidth = 12;
    const maxVisible = Math.max(20, Math.floor((chartWidth - padding * 2) / minSlotWidth));
    const drawCandles = candles.slice(-maxVisible);
    const slotWidth = Math.max(
      minSlotWidth,
      Math.min(maxSlotWidth, (chartWidth - padding * 2) / Math.max(1, drawCandles.length))
    );
    const bodyWidth = Math.max(2, Math.min(10, slotWidth * 0.66));
    const drawWidth = drawCandles.length * slotWidth;
    const startX = padding + Math.max(0, (chartWidth - padding * 2) - drawWidth);

    ctx.fillStyle = "#f4efd9";
    ctx.fillRect(0, 0, size.width, size.height);
    ctx.fillStyle = "rgba(255,255,255,0.18)";
    ctx.fillRect(chartWidth, 0, axisWidth, chartHeight);
    ctx.strokeStyle = "rgba(0,0,0,0.08)";
    ctx.beginPath();
    ctx.moveTo(chartWidth + 0.5, 0);
    ctx.lineTo(chartWidth + 0.5, chartHeight);
    ctx.stroke();

    if (!drawCandles.length) {
      ctx.fillStyle = "#5f6672";
      ctx.font = "14px Segoe UI";
      ctx.fillText("Waiting for candle data...", 14, 28);
      return;
    }

    const minLow = Math.min(...drawCandles.map((item) => item.low));
    const maxHigh = Math.max(...drawCandles.map((item) => item.high));
    const range = Math.max(0.0001, maxHigh - minLow);
    const paddedMin = minLow - range * 0.08;
    const paddedMax = maxHigh + range * 0.08;
    const paddedRange = Math.max(0.0001, paddedMax - paddedMin);

    function priceToY(price: number): number {
      const ratio = (paddedMax - price) / paddedRange;
      return Math.max(0, Math.min(chartHeight, ratio * chartHeight));
    }

    ctx.strokeStyle = "rgba(0,0,0,0.06)";
    ctx.fillStyle = "#5f6672";
    ctx.font = "12px Segoe UI";
    for (let i = 0; i <= 7; i += 1) {
      const ratio = i / 7;
      const y = ratio * chartHeight;
      ctx.beginPath();
      ctx.moveTo(0, y + 0.5);
      ctx.lineTo(chartWidth, y + 0.5);
      ctx.stroke();
      const price = paddedMax - paddedRange * ratio;
      ctx.fillText(price.toFixed(4), chartWidth + 6, y + 4);
    }

    for (let index = 0; index < drawCandles.length; index += 1) {
      const candle = drawCandles[index];
      const x = startX + index * slotWidth + slotWidth / 2;
      const openY = priceToY(candle.open);
      const closeY = priceToY(candle.close);
      const highY = priceToY(candle.high);
      const lowY = priceToY(candle.low);
      const up = candle.close >= candle.open;
      const color = up ? "#02cc86" : "#ef4253";

      ctx.strokeStyle = color;
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.moveTo(x, highY);
      ctx.lineTo(x, lowY);
      ctx.stroke();

      const bodyTop = Math.min(openY, closeY);
      const bodyHeight = Math.max(1.5, Math.abs(closeY - openY));
      ctx.fillRect(x - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);
    }

    const currentPrice = snapshot?.last_trade_price || snapshot?.mark_price || 0;
    if (currentPrice > 0) {
      const y = priceToY(currentPrice);
      ctx.setLineDash([6, 6]);
      ctx.strokeStyle = "#08d47c";
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(chartWidth, y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "#111";
      ctx.fillRect(chartWidth - 4, y - 10, 82, 20);
      ctx.fillStyle = "#fff";
      ctx.fillText(currentPrice.toFixed(4), chartWidth + 4, y + 4);
    }
  }, [candles, size.height, size.width, snapshot?.last_trade_price, snapshot?.mark_price]);

  return (
    <section className="panel orderflow-panel">
      <div className="heatmap-toolbar">
        <div className="small-title" style={{ marginBottom: 0 }}>Candles</div>
        <div className="heatmap-timeframes">
          {timeframeOptions.map((item) => (
            <button
              key={item}
              className={item === timeframe ? "timeframe-btn active" : "timeframe-btn"}
              onClick={() => setTimeframe(item)}
              type="button"
            >
              {item}
            </button>
          ))}
        </div>
      </div>
      <div ref={containerRef} className="candles-only-wrap">
        <canvas ref={canvasRef} className="candles-only-canvas" />
      </div>
    </section>
  );
}
