import { useMemo, useState } from "react";
import { HeatmapViewportState } from "../types/market";

export function useHeatmapViewport(totalColumns: number, plotWidth: number) {
  const [state, setState] = useState<HeatmapViewportState>({
    follow_live: true,
    column_width: 6,
    offset_columns: 0,
  });

  const visibleColumns = useMemo(() => {
    const width = Math.max(2, state.column_width);
    return Math.max(20, Math.floor(plotWidth / width));
  }, [plotWidth, state.column_width]);

  const startIndex = useMemo(() => {
    if (totalColumns <= visibleColumns) {
      return 0;
    }
    if (state.follow_live) {
      return totalColumns - visibleColumns;
    }
    const maxOffset = Math.max(0, totalColumns - visibleColumns);
    const offset = Math.max(0, Math.min(maxOffset, state.offset_columns));
    return Math.max(0, totalColumns - visibleColumns - offset);
  }, [state.follow_live, state.offset_columns, totalColumns, visibleColumns]);

  function zoom(delta: number) {
    setState((prev) => {
      const nextWidth = Math.max(2, Math.min(20, prev.column_width + delta));
      return { ...prev, column_width: nextWidth };
    });
  }

  function pan(columnsDelta: number) {
    setState((prev) => {
      const nextOffset = Math.max(0, prev.offset_columns + columnsDelta);
      return { ...prev, follow_live: false, offset_columns: nextOffset };
    });
  }

  function resetViewport() {
    setState({
      follow_live: true,
      column_width: 6,
      offset_columns: 0,
    });
  }

  return {
    viewport: state,
    visibleColumns,
    startIndex,
    zoom,
    pan,
    resetViewport,
  };
}
