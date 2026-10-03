import { useEffect, useMemo, useRef } from "react";
import { HeatmapColumn, KlinePoint, TerminalSnapshot } from "../../types/market";

type OverlayCell = {
  columnIndex: number;
  rowIndex: number;
  x: number;
  y: number;
};

type HeatmapOverlayProps = {
  width: number;
  height: number;
  bucketSize: number;
  rowMin: number;
  rowMax: number;
  startIndex: number;
  visibleColumns: number;
  visibleColumnCount: number;
  columnWidth: number;
  xOffset: number;
  timeBucketMs: number;
  columns: HeatmapColumn[];
  klineSeries: KlinePoint[];
  snapshot: TerminalSnapshot | null;
  timeframeMs: number;
  hoverCell: OverlayCell | null;
  lockedCell: OverlayCell | null;
  onPointerMove: (cell: OverlayCell | null) => void;
  onPointerDown: (cell: OverlayCell | null) => void;
  onPointerUp: () => void;
  onClick: (cell: OverlayCell | null) => void;
};

type Candle = {
  ts_ms: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

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

export default function HeatmapOverlay(props: HeatmapOverlayProps) {
  const {
    width,
    height,
    bucketSize,
    rowMin,
    rowMax,
    startIndex,
    visibleColumns,
    visibleColumnCount,
    columnWidth,
    xOffset,
    timeBucketMs,
    columns,
    klineSeries,
    snapshot,
    timeframeMs,
    hoverCell,
    lockedCell,
    onPointerMove,
    onPointerDown,
    onPointerUp,
    onClick,
  } = props;
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const candles = useMemo(() => aggregateCandles(klineSeries, timeframeMs), [klineSeries, timeframeMs]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || width <= 0 || height <= 0 || bucketSize <= 0) return;
    canvas.width = width;
    canvas.height = height;
    const context = canvas.getContext("2d");
    if (!context) return;

    context.clearRect(0, 0, width, height);

    const totalRows = Math.max(1, rowMax - rowMin + 1);
    const cellHeight = height / totalRows;
    const visibleSlice = columns.slice(startIndex, startIndex + visibleColumns);
    if (visibleSlice.length === 0) return;

    const firstTs = visibleSlice[0].ts_ms;
    const lastTs = visibleSlice[visibleSlice.length - 1].ts_ms;
    const dataWidth = Math.max(1, visibleColumnCount * columnWidth);
    const windowStartTs = firstTs;
    const windowEndTs = lastTs + timeBucketMs;

    function priceToBucketRow(price: number) {
      if (!Number.isFinite(price) || bucketSize <= 0) return rowMax;
      return Math.floor(price / bucketSize);
    }

    function rowCenterY(row: number) {
      const y = (rowMax - row + 0.5) * cellHeight;
      return Math.max(-5, Math.min(height + 5, y));
    }

    function rowTopY(row: number) {
      return (rowMax - row) * cellHeight;
    }

    function rowBottomY(row: number) {
      return rowTopY(row) + cellHeight;
    }

    function clampRow(row: number) {
      return Math.max(rowMin, Math.min(rowMax, row));
    }

    context.lineWidth = 1;
    const drawCandles = candles.filter((candle) => {
      const candleStart = candle.ts_ms;
      const candleEnd = candle.ts_ms + timeframeMs;
      return !(candleEnd < windowStartTs || candleStart > windowEndTs);
    });

    if (drawCandles.length > 0) {
      // Auto-fit active range so candles stay dense/readable even with sparse updates.
      const minSlotWidth = 4;
      const maxSlotWidth = 14;
      const slotWidth = Math.max(minSlotWidth, Math.min(maxSlotWidth, dataWidth / Math.max(1, drawCandles.length)));
      const seriesWidth = drawCandles.length * slotWidth;
      const seriesStartX = xOffset + Math.max(0, dataWidth - seriesWidth);
      const bodyWidth = Math.max(2, Math.min(12, slotWidth * 0.68));

      for (let index = 0; index < drawCandles.length; index += 1) {
        const candle = drawCandles[index];
        const wickX = seriesStartX + index * slotWidth + slotWidth / 2;
      const openRow = priceToBucketRow(candle.open);
      const highRow = priceToBucketRow(candle.high);
      const lowRow = priceToBucketRow(candle.low);
      const closeRow = priceToBucketRow(candle.close);
      const candleRowMax = Math.max(openRow, highRow, lowRow, closeRow);
      const candleRowMin = Math.min(openRow, highRow, lowRow, closeRow);
      if (candleRowMax < rowMin || candleRowMin > rowMax) continue;

      const openRowClamped = clampRow(openRow);
      const highRowClamped = clampRow(highRow);
      const lowRowClamped = clampRow(lowRow);
      const closeRowClamped = clampRow(closeRow);
      const highY = rowCenterY(highRowClamped);
      const lowY = rowCenterY(lowRowClamped);
      const bodyHighRow = Math.max(openRowClamped, closeRowClamped);
      const bodyLowRow = Math.min(openRowClamped, closeRowClamped);
      const bodyTop = rowTopY(bodyHighRow);
      const bodyBottom = rowBottomY(bodyLowRow);
      const isUp = candle.close >= candle.open;
      const color = isUp ? "#02cc86" : "#ef4253";
      const bodyHeight = Math.max(1, bodyBottom - bodyTop);

      context.strokeStyle = color;
      context.fillStyle = color;
      context.beginPath();
      context.moveTo(wickX, highY);
      context.lineTo(wickX, lowY);
      context.stroke();
      context.fillRect(wickX - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);
      }
    }

    const currentPrice = snapshot?.last_trade_price || snapshot?.mark_price || 0;
    if (currentPrice > 0) {
      const lineY = rowCenterY(clampRow(priceToBucketRow(currentPrice)));
      context.strokeStyle = "#08d47c";
      context.setLineDash([6, 6]);
      context.beginPath();
      context.moveTo(xOffset, lineY);
      context.lineTo(xOffset + dataWidth, lineY);
      context.stroke();
      context.setLineDash([]);
    }

    const target = lockedCell ?? hoverCell;
    if (target) {
      context.strokeStyle = "rgba(255,255,255,0.85)";
      context.lineWidth = 1;
      context.setLineDash([4, 4]);
      context.beginPath();
      context.moveTo(target.x, 0);
      context.lineTo(target.x, height);
      context.moveTo(0, target.y);
      context.lineTo(width, target.y);
      context.stroke();
      context.setLineDash([]);
    }
  }, [
    width,
    height,
    bucketSize,
    rowMin,
    rowMax,
    startIndex,
    visibleColumns,
    visibleColumnCount,
    columnWidth,
    xOffset,
    timeBucketMs,
    columns,
    candles,
    snapshot,
    timeframeMs,
    hoverCell,
    lockedCell,
  ]);

  function pointToCell(clientX: number, clientY: number): OverlayCell | null {
    const canvas = canvasRef.current;
    if (!canvas) return null;
    const rect = canvas.getBoundingClientRect();
    const x = clientX - rect.left;
    const y = clientY - rect.top;
    if (x < 0 || x > rect.width || y < 0 || y > rect.height) return null;
    if (x < xOffset || x > xOffset + visibleColumnCount * columnWidth) return null;
    const colOffset = Math.floor((x - xOffset) / Math.max(1, columnWidth));
    const columnIndex = startIndex + colOffset;
    if (columnIndex < 0 || columnIndex >= columns.length) return null;
    const totalRows = Math.max(1, rowMax - rowMin + 1);
    const cellHeight = rect.height / totalRows;
    const rowIndex = rowMax - Math.floor(y / Math.max(1, cellHeight));
    return {
      columnIndex,
      rowIndex,
      x,
      y,
    };
  }

  return (
    <canvas
      ref={canvasRef}
      className="heatmap-overlay-canvas"
      onPointerMove={(event) => onPointerMove(pointToCell(event.clientX, event.clientY))}
      onPointerLeave={() => onPointerMove(null)}
      onPointerDown={(event) => onPointerDown(pointToCell(event.clientX, event.clientY))}
      onPointerUp={() => onPointerUp()}
      onClick={(event) => onClick(pointToCell(event.clientX, event.clientY))}
    />
  );
}
