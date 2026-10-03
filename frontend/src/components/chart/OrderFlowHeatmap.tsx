import { memo, useEffect, useMemo, useRef, useState } from "react";
import { HeatmapColumn, KlinePoint, LadderSnapshot, TerminalSnapshot } from "../../types/market";
import { useHeatmapThresholds } from "../../hooks/useHeatmapThresholds";
import { useHeatmapViewport } from "../../hooks/useHeatmapViewport";
import { useHeatmapInteraction } from "../../hooks/useHeatmapInteraction";
import HeatmapToolbar from "./HeatmapToolbar";
import HeatmapOverlay from "./HeatmapOverlay";
import HeatmapInspector from "./HeatmapInspector";
import PriceAxis from "./PriceAxis";
import PriceLadderPanel from "./PriceLadderPanel";

type HeatmapStateView = {
  symbol: string;
  bucket_size: number;
  ticks_per_bucket: number;
  time_bucket_ms: number;
  max_columns: number;
  columns: HeatmapColumn[];
  ladder: LadderSnapshot | null;
};

type OrderFlowHeatmapProps = {
  klineSeries: KlinePoint[];
  snapshot: TerminalSnapshot | null;
  heatmap: HeatmapStateView | null;
};

const ROW_SCALE_OPTIONS = [1, 2, 4];

function colorFromIntensity(intensity: number): string {
  const stops = [
    [245, 238, 210],
    [237, 186, 168],
    [206, 98, 118],
    [120, 44, 119],
  ];
  const clamped = Math.max(0, Math.min(1, intensity));
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

function valueFromColumnRow(column: HeatmapColumn | undefined, row: number): number {
  if (!column) return 0;
  const item = column.rows.find((cell) => cell.row === row);
  return item ? item.value : 0;
}

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

function aggregateHeatmapColumns(columns: HeatmapColumn[], sourceBucketMs: number, targetBucketMs: number): HeatmapColumn[] {
  if (columns.length === 0) return columns;
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
      // Preserve strong resting levels per time bucket.
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

function aggregateHeatmapRows(columns: HeatmapColumn[], rowScale: number): HeatmapColumn[] {
  if (columns.length === 0 || rowScale <= 1) return columns;
  return columns.map((column) => {
    const grouped = new Map<number, number>();
    for (const row of column.rows) {
      const groupRow = Math.floor(row.row / rowScale);
      const prev = grouped.get(groupRow) ?? 0;
      grouped.set(groupRow, prev + row.value);
    }
    return {
      ts_ms: column.ts_ms,
      center_row: Math.floor(column.center_row / rowScale),
      row_min: Math.floor(column.row_min / rowScale),
      row_max: Math.floor(column.row_max / rowScale),
      rows: [...grouped.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([row, value]) => ({ row, value })),
    };
  });
}

function OrderFlowHeatmap({ klineSeries, snapshot, heatmap }: OrderFlowHeatmapProps) {
  const defaultRowWindow = 720;
  const timeframeOptions = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"];
  const rowScaleOptions = ROW_SCALE_OPTIONS;
  const dataAreaRatio = 0.72;
  const chartRef = useRef<HTMLDivElement | null>(null);
  const heatmapCanvasRef = useRef<HTMLCanvasElement | null>(null);
  const panningRef = useRef<{ active: boolean; lastX: number; lastY: number; moved: boolean }>({
    active: false,
    lastX: 0,
    lastY: 0,
    moved: false,
  });
  const suppressClickRef = useRef(false);
  const [chartSize, setChartSize] = useState({ width: 900, height: 520 });
  const [isFullscreenFallback, setIsFullscreenFallback] = useState(false);
  const [timeframe, setTimeframe] = useState("1m");
  const [rowScale, setRowScale] = useState(2);
  const [rowCenter, setRowCenter] = useState<number | null>(null);
  const [rowWindow, setRowWindow] = useState(defaultRowWindow);
  const { low, high, setLow, setHigh, reset } = useHeatmapThresholds();

  useEffect(() => {
    if (!rowScaleOptions.includes(rowScale)) {
      setRowScale(2);
      setRowCenter(null);
    }
  }, [rowScale, rowScaleOptions]);

  const rawColumns = heatmap?.columns ?? [];
  const bucketSize = heatmap?.bucket_size ?? 0;
  const sourceTimeBucketMs = heatmap?.time_bucket_ms ?? 500;
  const ladder = heatmap?.ladder ?? null;
  const timeframeMs = useMemo(() => timeframeToMs(timeframe), [timeframe]);
  const renderTimeBucketMs = Math.max(sourceTimeBucketMs, timeframeMs);
  const timeAggregatedColumns = useMemo(
    () => aggregateHeatmapColumns(rawColumns, sourceTimeBucketMs, renderTimeBucketMs),
    [rawColumns, sourceTimeBucketMs, renderTimeBucketMs]
  );
  const columns = useMemo(() => aggregateHeatmapRows(timeAggregatedColumns, rowScale), [timeAggregatedColumns, rowScale]);
  const displayBucketSize = bucketSize > 0 ? bucketSize * rowScale : 0;

  const plotWidth = Math.max(240, chartSize.width - 90);
  const plotHeight = Math.max(240, chartSize.height);
  const dataAreaWidth = Math.max(180, Math.floor(plotWidth * dataAreaRatio));

  const { viewport, visibleColumns, startIndex, zoom, pan, resetViewport } = useHeatmapViewport(columns.length, dataAreaWidth);
  const { hoverCell, inspector, onHover, onClick, clearLock } = useHeatmapInteraction();

  const { globalRowMin, globalRowMax } = useMemo(() => {
    if (columns.length === 0) return { globalRowMin: 0, globalRowMax: 1 };
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    for (const column of columns) {
      if (column.row_min < min) min = column.row_min;
      if (column.row_max > max) max = column.row_max;
    }
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return { globalRowMin: 0, globalRowMax: 1 };
    }
    return { globalRowMin: min, globalRowMax: max };
  }, [columns]);

  useEffect(() => {
    if (columns.length === 0) return;
    const refPrice = snapshot?.last_trade_price || snapshot?.mark_price || 0;
    const refRow = displayBucketSize > 0 && refPrice > 0
      ? Math.round(refPrice / displayBucketSize)
      : Math.round((globalRowMin + globalRowMax) / 2);
    if (rowCenter === null) {
      setRowCenter(refRow);
    }
  }, [displayBucketSize, columns.length, globalRowMax, globalRowMin, rowCenter, snapshot?.last_trade_price, snapshot?.mark_price]);

  useEffect(() => {
    if (rowCenter === null) return;
    if (rowCenter < globalRowMin || rowCenter > globalRowMax) {
      setRowCenter(Math.round((globalRowMin + globalRowMax) / 2));
    }
  }, [globalRowMax, globalRowMin, rowCenter]);

  const effectiveCenter = rowCenter ?? Math.round((globalRowMin + globalRowMax) / 2);
  const fullSpan = Math.max(2, globalRowMax - globalRowMin + 1);
  const clampedWindow = Math.max(80, Math.min(1200, rowWindow, fullSpan));
  const halfWindow = Math.floor(clampedWindow / 2);
  const viewRowMin = Math.max(globalRowMin, effectiveCenter - halfWindow);
  const viewRowMax = Math.min(globalRowMax, effectiveCenter + halfWindow);
  const totalRows = Math.max(2, viewRowMax - viewRowMin + 1);
  const visibleSlice = columns.slice(startIndex, startIndex + visibleColumns);
  const visibleColumnCount = visibleSlice.length;
  // Keep plot anchored from the left edge so early/short history does not appear detached.
  const xOffset = 0;
  const renderColumnWidth = viewport.column_width;

  const targetCell = inspector.locked && inspector.column_index !== null && inspector.row_index !== null
    ? {
        columnIndex: inspector.column_index,
        rowIndex: inspector.row_index,
        x: hoverCell?.x ?? 10,
        y: hoverCell?.y ?? 10,
      }
    : hoverCell;

  const inspectorMatrix = useMemo(() => {
    if (!targetCell) return [];
    const matrix: { value: number; row: number; ts_ms: number }[][] = [];
    for (let rowOffset = 2; rowOffset >= -2; rowOffset -= 1) {
      const row = targetCell.rowIndex + rowOffset;
      const rowCells: { value: number; row: number; ts_ms: number }[] = [];
      for (let colOffset = -1; colOffset <= 1; colOffset += 1) {
        const columnIndex = targetCell.columnIndex + colOffset;
        const column = columns[columnIndex];
        rowCells.push({
          value: valueFromColumnRow(column, row),
          row,
          ts_ms: column?.ts_ms ?? 0,
        });
      }
      matrix.push(rowCells);
    }
    return matrix;
  }, [columns, targetCell]);

  useEffect(() => {
    const element = chartRef.current;
    if (!element) return;
    const observer = new ResizeObserver((entries) => {
      const contentRect = entries[0]?.contentRect;
      if (!contentRect) return;
      setChartSize({
        width: Math.max(280, Math.floor(contentRect.width)),
        height: Math.max(260, Math.floor(contentRect.height)),
      });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const element = chartRef.current;
    if (!element) return;
    const onWheelNative = (event: WheelEvent) => {
      event.preventDefault();
      if (event.ctrlKey || event.altKey) {
        setRowWindow((prev) => {
          const delta = event.deltaY > 0 ? 24 : -24;
          return Math.max(80, Math.min(1200, prev + delta));
        });
      } else if (event.shiftKey) {
        pan(event.deltaY > 0 ? 5 : -5);
      } else {
        zoom(event.deltaY > 0 ? -1 : 1);
      }
    };
    element.addEventListener("wheel", onWheelNative, { passive: false });
    return () => element.removeEventListener("wheel", onWheelNative);
  }, [pan, zoom]);

  useEffect(() => {
    if (!isFullscreenFallback) return;
    const onEsc = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsFullscreenFallback(false);
      }
    };
    window.addEventListener("keydown", onEsc);
    return () => window.removeEventListener("keydown", onEsc);
  }, [isFullscreenFallback]);

  async function toggleFullscreen() {
    setIsFullscreenFallback((prev) => !prev);
  }

  function beginMousePan(clientX: number, clientY: number) {
    panningRef.current = { active: true, lastX: clientX, lastY: clientY, moved: false };
  }

  function updateMousePan(clientX: number, clientY: number) {
    const state = panningRef.current;
    if (!state.active) return;
    const step = Math.max(1, renderColumnWidth);
    const columnsDelta = Math.trunc((state.lastX - clientX) / step);
    if (columnsDelta !== 0) {
      pan(columnsDelta);
      panningRef.current.moved = true;
      panningRef.current.lastX = clientX;
    }

    const pxPerRow = Math.max(1, plotHeight / Math.max(1, totalRows));
    const rowsDelta = Math.trunc((clientY - state.lastY) / pxPerRow);
    if (rowsDelta !== 0) {
      setRowCenter((prev) => {
        const current = prev ?? effectiveCenter;
        const next = Math.max(globalRowMin, Math.min(globalRowMax, current + rowsDelta));
        return next;
      });
      panningRef.current.moved = true;
      panningRef.current.lastY = clientY;
    }
  }

  function endMousePan() {
    if (panningRef.current.moved) {
      suppressClickRef.current = true;
      window.setTimeout(() => {
        suppressClickRef.current = false;
      }, 80);
    }
    panningRef.current.active = false;
  }

  function handleOverlayClick(cell: { columnIndex: number; rowIndex: number; x: number; y: number } | null) {
    if (suppressClickRef.current) return;
    onClick(cell);
  }

  function handleResetView() {
    resetViewport();
    setRowWindow(defaultRowWindow);
    setRowCenter(null);
  }

  function zoomPriceIn() {
    setRowWindow((prev) => Math.max(80, prev - 48));
  }

  function zoomPriceOut() {
    setRowWindow((prev) => Math.min(1200, prev + 48));
  }

  useEffect(() => {
    const canvas = heatmapCanvasRef.current;
    if (!canvas) return;
    const width = plotWidth;
    const height = plotHeight;
    canvas.width = width;
    canvas.height = height;
    const context = canvas.getContext("2d");
    if (!context) return;

    context.fillStyle = "#f4efd9";
    context.fillRect(0, 0, width, height);
    if (columns.length === 0 || bucketSize <= 0) return;

    const totalRowsSafe = Math.max(1, viewRowMax - viewRowMin + 1);
    const cellHeight = height / totalRowsSafe;
    const maxLog = Math.max(
      1,
      ...visibleSlice.flatMap((column) => column.rows.map((row) => Math.log1p(row.value)))
    );
    const lowCut = low * maxLog;
    const highCut = Math.max(lowCut + 0.001, high * maxLog);
    const persistenceDecay = 0.92;
    const minPersistValue = 0.01;
    const persistedByRow = new Map<number, number>();

    for (let columnOffset = 0; columnOffset < visibleSlice.length; columnOffset += 1) {
      const column = visibleSlice[columnOffset];
      const x = xOffset + columnOffset * renderColumnWidth;
      const drawWidth = renderColumnWidth;
      const rawByRow = new Map<number, number>();
      for (const rowValue of column.rows) {
        rawByRow.set(rowValue.row, rowValue.value);
      }

      for (let row = viewRowMin; row <= viewRowMax; row += 1) {
        const raw = rawByRow.get(row);
        if (raw !== undefined) {
          persistedByRow.set(row, raw);
        } else {
          const prev = persistedByRow.get(row) ?? 0;
          const decayed = prev * persistenceDecay;
          if (decayed > minPersistValue) {
            persistedByRow.set(row, decayed);
          } else {
            persistedByRow.delete(row);
          }
        }

        const value = persistedByRow.get(row);
        if (!value || value <= 0) continue;
        const y = (viewRowMax - row) * cellHeight;
        const transformed = Math.log1p(value);
        const intensity = Math.max(0, Math.min(1, (transformed - lowCut) / (highCut - lowCut)));
        context.fillStyle = colorFromIntensity(intensity);
        context.fillRect(x, y, drawWidth + 0.4, Math.max(1, cellHeight + 0.4));
      }
    }
  }, [
    bucketSize,
    columns,
    high,
    low,
    plotHeight,
    plotWidth,
    viewRowMax,
    viewRowMin,
    visibleSlice,
    renderColumnWidth,
    xOffset,
  ]);

  return (
    <section className="panel orderflow-panel">
      <HeatmapToolbar
        low={low}
        high={high}
        onLowChange={setLow}
        onHighChange={setHigh}
        onReset={reset}
        onResetViewport={handleResetView}
        timeframe={timeframe}
        timeframes={timeframeOptions}
        onTimeframeChange={setTimeframe}
        rowScale={rowScale}
        rowScaleOptions={rowScaleOptions}
        onRowScaleChange={(value) => {
          setRowScale(value);
          setRowCenter(null);
        }}
        onPriceZoomIn={zoomPriceIn}
        onPriceZoomOut={zoomPriceOut}
      />
      <div className="orderflow-content">
        <div
          className={isFullscreenFallback ? "orderflow-chart is-fullscreen-fallback" : "orderflow-chart"}
          ref={chartRef}
          onContextMenu={(event) => event.preventDefault()}
          onPointerDown={(event) => {
            const target = event.target as HTMLElement;
            if (target.closest(".heatmap-fullscreen-btn, .inspector-close, .heatmap-toolbar, button, input")) {
              return;
            }
            if (event.button === 0 || event.button === 2) {
              event.currentTarget.setPointerCapture(event.pointerId);
              beginMousePan(event.clientX, event.clientY);
            }
          }}
          onPointerMove={(event) => {
            if (panningRef.current.active) {
              updateMousePan(event.clientX, event.clientY);
            }
          }}
          onPointerUp={endMousePan}
          onPointerCancel={endMousePan}
          onPointerLeave={endMousePan}
        >
          <canvas ref={heatmapCanvasRef} className="heatmap-canvas" />
          <HeatmapOverlay
            width={plotWidth}
            height={plotHeight}
            bucketSize={displayBucketSize}
            rowMin={viewRowMin}
            rowMax={viewRowMax}
            startIndex={startIndex}
            visibleColumns={visibleColumns}
            visibleColumnCount={visibleColumnCount}
            columnWidth={renderColumnWidth}
            xOffset={xOffset}
            timeBucketMs={renderTimeBucketMs}
            columns={columns}
            klineSeries={klineSeries}
            snapshot={snapshot}
            timeframeMs={timeframeMs}
            hoverCell={hoverCell}
            lockedCell={inspector.locked && targetCell ? targetCell : null}
            onPointerMove={onHover}
            onPointerDown={() => {}}
            onPointerUp={() => {}}
            onClick={handleOverlayClick}
          />
          <PriceAxis rowMin={viewRowMin} rowMax={viewRowMax} bucketSize={displayBucketSize} currentPrice={snapshot?.last_trade_price ?? 0} />
          <HeatmapInspector
            visible={Boolean(targetCell)}
            anchorY={targetCell?.y ?? 10}
            chartHeight={plotHeight}
            matrix={inspectorMatrix}
          />
          <button className="heatmap-fullscreen-btn" onClick={toggleFullscreen}>
            {isFullscreenFallback ? "Exit Full" : "Full Screen"}
          </button>
          {inspector.locked ? (
            <button className="inspector-close" onClick={clearLock}>
              Unlock
            </button>
          ) : null}
          <div className="heatmap-status">
            <span>{heatmap?.symbol ?? "-"}</span>
            <span>{columns.length} cols</span>
            <span>{timeframe} view</span>
            <span>row {rowScale}x</span>
            <span>{totalRows} rows</span>
          </div>
        </div>
        <PriceLadderPanel ladder={ladder} />
      </div>
    </section>
  );
}

export default memo(OrderFlowHeatmap);
