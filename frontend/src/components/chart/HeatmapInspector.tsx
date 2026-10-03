import { useMemo } from "react";

type InspectorCell = {
  value: number;
  row: number;
  ts_ms: number;
};

type HeatmapInspectorProps = {
  visible: boolean;
  anchorY: number;
  chartHeight: number;
  matrix: InspectorCell[][];
};

function formatLiquidity(value: number) {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(1);
}

export default function HeatmapInspector({ visible, anchorY, chartHeight, matrix }: HeatmapInspectorProps) {
  if (!visible || matrix.length === 0) return null;

  const inspectorHeight = 272;
  const padding = 10;
  const top = useMemo(() => {
    const raw = anchorY - inspectorHeight / 2;
    const maxTop = Math.max(padding, chartHeight - inspectorHeight - padding);
    return Math.max(padding, Math.min(raw, maxTop));
  }, [anchorY, chartHeight]);

  return (
    <div className="heatmap-inspector" style={{ left: 10, top }}>
      <table>
        <tbody>
          {matrix.map((rowCells, rowIndex) => (
            <tr key={`inspector-row-${rowIndex}`}>
              {rowCells.map((cell, colIndex) => (
                <td key={`inspector-cell-${rowIndex}-${colIndex}`} className={rowIndex === 2 && colIndex === 1 ? "active" : ""}>
                  <div>{formatLiquidity(cell.value)}</div>
                  {colIndex === rowCells.length - 1 ? <div className="meta">{cell.row}</div> : null}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
