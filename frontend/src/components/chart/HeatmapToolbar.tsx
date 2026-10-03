type HeatmapToolbarProps = {
  low: number;
  high: number;
  onLowChange: (value: number) => void;
  onHighChange: (value: number) => void;
  onReset: () => void;
  onResetViewport: () => void;
  timeframe: string;
  timeframes: string[];
  onTimeframeChange: (value: string) => void;
  rowScale: number;
  rowScaleOptions: number[];
  onRowScaleChange: (value: number) => void;
  onPriceZoomIn: () => void;
  onPriceZoomOut: () => void;
};

export default function HeatmapToolbar({
  low,
  high,
  onLowChange,
  onHighChange,
  onReset,
  onResetViewport,
  timeframe,
  timeframes,
  onTimeframeChange,
  rowScale,
  rowScaleOptions,
  onRowScaleChange,
  onPriceZoomIn,
  onPriceZoomOut,
}: HeatmapToolbarProps) {
  return (
    <div className="heatmap-toolbar">
      <div className="heatmap-toolbar-item">
        <span>Low</span>
        <input type="range" min={0} max={1} step={0.01} value={low} onChange={(event) => onLowChange(Number(event.target.value))} />
      </div>
      <div className="heatmap-toolbar-item">
        <span>High</span>
        <input type="range" min={0} max={1} step={0.01} value={high} onChange={(event) => onHighChange(Number(event.target.value))} />
      </div>
      <button onClick={onReset}>Reset Thresholds</button>
      <button onClick={onResetViewport}>Reset View</button>
      <div className="heatmap-price-controls">
        <button type="button" className="timeframe-btn" onClick={onPriceZoomOut}>Price -</button>
        <button type="button" className="timeframe-btn" onClick={onPriceZoomIn}>Price +</button>
      </div>
      <div className="heatmap-timeframes">
        {timeframes.map((item) => (
          <button
            key={item}
            className={item === timeframe ? "timeframe-btn active" : "timeframe-btn"}
            onClick={() => onTimeframeChange(item)}
            type="button"
          >
            {item}
          </button>
        ))}
      </div>
      <div className="heatmap-timeframes">
        {rowScaleOptions.map((item) => (
          <button
            key={`row-scale-${item}`}
            className={item === rowScale ? "timeframe-btn active" : "timeframe-btn"}
            onClick={() => onRowScaleChange(item)}
            type="button"
            title={item === 1 ? "Row Size 1x (fine)" : `Row Size ${item}x (more price range per row)`}
          >
            {item}x
          </button>
        ))}
      </div>
    </div>
  );
}
