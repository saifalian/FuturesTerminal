type PriceAxisProps = {
  rowMin: number;
  rowMax: number;
  bucketSize: number;
  currentPrice: number;
};

export default function PriceAxis({ rowMin, rowMax, bucketSize, currentPrice }: PriceAxisProps) {
  const steps = 8;
  const labels = Array.from({ length: steps + 1 }).map((_, index) => {
    const ratio = index / steps;
    const row = rowMax - Math.round((rowMax - rowMin) * ratio);
    return {
      ratio,
      price: row * bucketSize,
    };
  });

  return (
    <div className="price-axis">
      {labels.map((label) => (
        <div key={`${label.ratio}-${label.price}`} className="price-axis-label" style={{ top: `${label.ratio * 100}%` }}>
          {label.price.toFixed(4)}
        </div>
      ))}
      <div className="price-axis-current">{currentPrice > 0 ? currentPrice.toFixed(4) : "-"}</div>
    </div>
  );
}
