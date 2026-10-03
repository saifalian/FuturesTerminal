import { useState } from "react";
import { HeatmapInspectorState } from "../types/market";

type CellRef = {
  columnIndex: number;
  rowIndex: number;
  x: number;
  y: number;
};

export function useHeatmapInteraction() {
  const [hoverCell, setHoverCell] = useState<CellRef | null>(null);
  const [inspector, setInspector] = useState<HeatmapInspectorState>({
    locked: false,
    column_index: null,
    row_index: null,
  });
  const [dragging, setDragging] = useState(false);

  function onHover(cell: CellRef | null) {
    setHoverCell(cell);
    if (dragging && cell) {
      setInspector({
        locked: true,
        column_index: cell.columnIndex,
        row_index: cell.rowIndex,
      });
    }
  }

  function onClick(cell: CellRef | null) {
    if (!cell) return;
    setInspector({
      locked: true,
      column_index: cell.columnIndex,
      row_index: cell.rowIndex,
    });
  }

  function onPointerDown(cell: CellRef | null) {
    setDragging(true);
    if (cell) {
      setInspector({
        locked: true,
        column_index: cell.columnIndex,
        row_index: cell.rowIndex,
      });
    }
  }

  function onPointerUp() {
    setDragging(false);
  }

  function clearLock() {
    setInspector({
      locked: false,
      column_index: null,
      row_index: null,
    });
  }

  return {
    hoverCell,
    inspector,
    dragging,
    onHover,
    onClick,
    onPointerDown,
    onPointerUp,
    clearLock,
  };
}
