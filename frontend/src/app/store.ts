import {
  HeatmapBootstrap,
  HeatmapColumn,
  KlinePoint,
  LadderSnapshot,
  MarketEvent,
  SourceCoverageUpdate,
  SymbolResolutionMessage,
  TerminalSnapshot,
  TerminalWsMessage,
} from "../types/market";

type Listener = () => void;

type HeatmapState = {
  symbol: string;
  bucket_size: number;
  ticks_per_bucket: number;
  time_bucket_ms: number;
  max_columns: number;
  columns: HeatmapColumn[];
  ladder: LadderSnapshot | null;
};

export type TerminalState = {
  events: MarketEvent[];
  klineSeries: KlinePoint[];
  snapshot: TerminalSnapshot | null;
  heatmap: HeatmapState | null;
  sourceCoverage: SourceCoverageUpdate | null;
  symbolResolution: SymbolResolutionMessage | null;
};

export class TerminalStore {
  private listeners = new Set<Listener>();
  private static readonly MAX_EVENT_COUNT = 300;
  private static readonly MAX_KLINE_POINTS = 5_000;
  private state: TerminalState = {
    events: [],
    klineSeries: [],
    snapshot: null,
    heatmap: null,
    sourceCoverage: null,
    symbolResolution: null,
  };

  subscribe(listener: Listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  getSnapshot(): TerminalState {
    return this.state;
  }

  reset() {
    this.state = {
      events: [],
      klineSeries: [],
      snapshot: null,
      heatmap: null,
      sourceCoverage: null,
      symbolResolution: null,
    };
    this.emit();
  }

  initHeatmap(payload: HeatmapBootstrap) {
    this.state = {
      ...this.state,
      heatmap: {
        symbol: payload.symbol,
        bucket_size: payload.bucket_size,
        ticks_per_bucket: payload.ticks_per_bucket,
        time_bucket_ms: payload.time_bucket_ms,
        max_columns: payload.max_columns,
        columns: payload.columns ?? [],
        ladder: payload.ladder ?? null,
      },
    };
    this.emit();
  }

  pushMessage(message: TerminalWsMessage) {
    if (message.type === "terminal_snapshot") {
      this.state = {
        ...this.state,
        snapshot: message,
      };
      this.emit();
      return;
    }

    if (message.type === "market_event") {
      const expectedSymbol = this.state.heatmap?.symbol ?? this.state.snapshot?.symbol ?? "";
      const incomingSymbol = String((message as { symbol?: string }).symbol ?? "").toUpperCase();
      if (expectedSymbol && incomingSymbol && incomingSymbol !== expectedSymbol) {
        // Defensive guard: ignore cross-symbol events that may arrive during fast tab/symbol handoffs.
        return;
      }
      if (!message.stream.includes("@kline_")) {
        // Skip high-frequency non-kline events to keep UI rendering smooth.
        return;
      }
      const nextKlineSeries = this.upsertKline(message);
      this.state = {
        ...this.state,
        events: [...this.state.events.slice(-(TerminalStore.MAX_EVENT_COUNT - 1)), message],
        klineSeries: nextKlineSeries,
      };
      this.emit();
      return;
    }

    if (message.type === "symbol_changed") {
      const existing = this.state.heatmap;
      this.state = {
        ...this.state,
        klineSeries: [],
        symbolResolution: null,
        heatmap: existing
          ? {
              ...existing,
              symbol: message.symbol,
              columns: [],
              ladder: null,
            }
          : null,
      };
      this.emit();
      return;
    }

    if (message.type === "source_coverage_update") {
      this.state = {
        ...this.state,
        sourceCoverage: message,
      };
      this.emit();
      return;
    }

    if (message.type === "symbol_resolution") {
      this.state = {
        ...this.state,
        symbolResolution: message,
      };
      this.emit();
      return;
    }

    if (message.type === "heatmap_frame") {
      const current = this.state.heatmap;
      const base: HeatmapState = current && current.symbol === message.symbol
        ? current
        : {
            symbol: message.symbol,
            bucket_size: message.bucket_size,
            ticks_per_bucket: message.ticks_per_bucket,
            time_bucket_ms: message.time_bucket_ms,
            max_columns: message.max_columns,
            columns: [],
            ladder: null,
          };
      const nextColumns = [...base.columns.slice(-(message.max_columns - 1)), message.column];
      this.state = {
        ...this.state,
        heatmap: {
          ...base,
          bucket_size: message.bucket_size,
          ticks_per_bucket: message.ticks_per_bucket,
          time_bucket_ms: message.time_bucket_ms,
          max_columns: message.max_columns,
          columns: nextColumns,
        },
      };
      this.emit();
      return;
    }

    if (message.type === "ladder_snapshot") {
      const current = this.state.heatmap;
      if (!current || current.symbol !== message.symbol) {
        this.state = {
          ...this.state,
          heatmap: {
            symbol: message.symbol,
            bucket_size: message.bucket_size,
            ticks_per_bucket: 2,
            time_bucket_ms: 500,
            max_columns: 14_400,
            columns: [],
            ladder: message,
          },
        };
      } else {
        this.state = {
          ...this.state,
          heatmap: {
            ...current,
            ladder: message,
          },
        };
      }
      this.emit();
      return;
    }
  }

  private upsertKline(message: MarketEvent): KlinePoint[] {
    const payload = message.payload as {
      k?: {
        t?: number | string;
        T?: number | string;
        o?: number | string;
        h?: number | string;
        l?: number | string;
        c?: number | string;
        i?: string;
        x?: boolean;
      };
    };
    const k = payload.k;
    if (!k) return this.state.klineSeries;

    const openTs = Number(k.t ?? 0);
    const closeTs = Number(k.T ?? 0);
    const open = Number(k.o ?? 0);
    const high = Number(k.h ?? 0);
    const low = Number(k.l ?? 0);
    const close = Number(k.c ?? 0);
    const interval = String(k.i ?? "1m");
    const isClosed = Boolean(k.x);

    if (![openTs, closeTs, open, high, low, close].every(Number.isFinite)) {
      return this.state.klineSeries;
    }

    const nextPoint: KlinePoint = {
      open_ts_ms: openTs,
      close_ts_ms: closeTs,
      open,
      high,
      low,
      close,
      interval,
      is_closed: isClosed,
    };

    const current = this.state.klineSeries;
    let index = -1;
    const last = current[current.length - 1];
    if (last && last.open_ts_ms === openTs) {
      index = current.length - 1;
    } else {
      index = current.findIndex((point) => point.open_ts_ms === openTs);
    }

    if (index === -1) {
      const appended = [...current, nextPoint];
      if (appended.length <= TerminalStore.MAX_KLINE_POINTS) {
        return appended;
      }
      return appended.slice(-TerminalStore.MAX_KLINE_POINTS);
    }

    const prev = current[index];
    const merged: KlinePoint = {
      open_ts_ms: prev.open_ts_ms,
      close_ts_ms: Math.max(prev.close_ts_ms, nextPoint.close_ts_ms),
      open: prev.open || nextPoint.open,
      high: Math.max(prev.high, nextPoint.high),
      low: Math.min(prev.low, nextPoint.low),
      close: nextPoint.close,
      interval: nextPoint.interval || prev.interval,
      is_closed: prev.is_closed || nextPoint.is_closed,
    };
    const updated = [...current];
    updated[index] = merged;
    return updated;
  }

  private emit() {
    for (const listener of this.listeners) listener();
  }
}

export function createTerminalStore() {
  return new TerminalStore();
}

export const store = new TerminalStore();
