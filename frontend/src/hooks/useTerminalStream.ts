import { useEffect, useSyncExternalStore } from "react";
import { createTerminalStore, TerminalState, TerminalStore } from "../app/store";
import { WsClient } from "../app/wsClient";
import { HeatmapBootstrap, SourceCoverageUpdate, TerminalSnapshot, TerminalWsMessage } from "../types/market";

type TerminalTabContext = {
  tabId: string;
  store: TerminalStore;
  ws: WsClient;
  initialized: boolean;
  initPromise: Promise<void> | null;
  expectedSymbol: string;
};

const TAB_CONTEXTS = new Map<string, TerminalTabContext>();

function normalizeTabId(tabId: string) {
  const value = (tabId || "default").trim().toLowerCase();
  if (!value) return "default";
  return value;
}

function endpoint(path: string, tabId: string) {
  const query = new URLSearchParams({ tab_id: tabId });
  return `http://127.0.0.1:8000${path}?${query.toString()}`;
}

function normalizeSymbol(symbol: string | null | undefined) {
  return String(symbol ?? "").trim().toUpperCase();
}

function extractMarketEventSymbol(message: TerminalWsMessage): string {
  if (message.type !== "market_event") return "";
  const payload = message.payload ?? {};
  const direct = normalizeSymbol((payload as { s?: string }).s);
  if (direct) return direct;
  const k = (payload as { k?: { s?: string } }).k;
  const fromK = normalizeSymbol(k?.s);
  if (fromK) return fromK;
  const o = (payload as { o?: { s?: string } }).o;
  const fromO = normalizeSymbol(o?.s);
  if (fromO) return fromO;
  return normalizeSymbol(message.symbol);
}

function extractMessageSymbol(message: TerminalWsMessage): string {
  if (message.type === "symbol_resolution") {
    return normalizeSymbol(message.normalized_symbol);
  }
  if (message.type === "market_event") {
    return extractMarketEventSymbol(message);
  }
  if ("symbol" in message) {
    return normalizeSymbol((message as { symbol?: string }).symbol);
  }
  return "";
}

function shouldAcceptMessage(ctx: TerminalTabContext, message: TerminalWsMessage): boolean {
  const expected = normalizeSymbol(ctx.expectedSymbol);
  if (!expected) return true;
  const incoming = extractMessageSymbol(message);
  if (!incoming) return true;
  return incoming === expected;
}

function getContext(tabId: string): TerminalTabContext {
  const normalized = normalizeTabId(tabId);
  const existing = TAB_CONTEXTS.get(normalized);
  if (existing) return existing;
  const created: TerminalTabContext = {
    tabId: normalized,
    store: createTerminalStore(),
    ws: new WsClient(normalized),
    initialized: false,
    initPromise: null,
    expectedSymbol: "",
  };
  TAB_CONTEXTS.set(normalized, created);
  return created;
}

async function bootstrapContext(ctx: TerminalTabContext) {
  // Priority-first bootstrap:
  // 1) get terminal snapshot immediately
  // 2) get source coverage
  // 3) load heavy heatmap bootstrap in background (non-blocking)
  const [snapshotResponse, coverageResponse] = await Promise.all([
    fetch(endpoint("/terminal/snapshot", ctx.tabId)),
    fetch(endpoint("/terminal/source-coverage", ctx.tabId)),
  ]);

  const snapshot = (await snapshotResponse.json()) as Partial<TerminalSnapshot>;
  if (snapshot.type === "terminal_snapshot") {
    const message = snapshot as TerminalSnapshot;
    if (shouldAcceptMessage(ctx, message)) {
      ctx.store.pushMessage(message);
    }
  }

  const coverage = (await coverageResponse.json()) as Partial<SourceCoverageUpdate>;
  if (coverage.type === "source_coverage_update") {
    const message = coverage as SourceCoverageUpdate;
    if (shouldAcceptMessage(ctx, message)) {
      ctx.store.pushMessage(message);
    }
  }

  // Do not block startup on heatmap bootstrap payload.
  void fetch(endpoint("/terminal/heatmap/bootstrap", ctx.tabId))
    .then((response) => response.json())
    .then((payload) => {
      const bootstrap = payload as Partial<HeatmapBootstrap>;
      if (bootstrap.type !== "heatmap_bootstrap") return;
      const expected = normalizeSymbol(ctx.expectedSymbol);
      const incoming = normalizeSymbol(bootstrap.symbol);
      if (!expected || !incoming || expected === incoming) {
        ctx.store.initHeatmap(bootstrap as HeatmapBootstrap);
      }
    })
    .catch(() => {});
}

function ensureContextInitialized(ctx: TerminalTabContext) {
  if (!ctx.initPromise) {
    ctx.initPromise = bootstrapContext(ctx)
      .catch(() => {})
      .finally(() => {
        ctx.initialized = true;
        ctx.initPromise = null;
      });
  }
  ctx.ws.connect((message) => {
    if (!shouldAcceptMessage(ctx, message)) {
      return;
    }
    ctx.store.pushMessage(message);
  });
}

export async function initializeTerminalTab(tabId: string) {
  const ctx = getContext(tabId);
  if (!ctx.initialized && !ctx.initPromise) {
    ctx.initPromise = bootstrapContext(ctx)
      .catch(() => {})
      .finally(() => {
        ctx.initialized = true;
        ctx.initPromise = null;
      });
  }
  if (ctx.initPromise) {
    await ctx.initPromise;
  }
}

export function setTerminalTabExpectedSymbol(tabId: string, symbol: string) {
  const ctx = getContext(tabId);
  ctx.expectedSymbol = normalizeSymbol(symbol);
  ctx.ws.setExpectedSymbol(ctx.expectedSymbol);
}

export function closeTerminalTabClient(tabId: string) {
  const normalized = normalizeTabId(tabId);
  const ctx = TAB_CONTEXTS.get(normalized);
  if (!ctx) return;
  ctx.ws.close();
  TAB_CONTEXTS.delete(normalized);
}

export function useTerminalStream(tabId: string, expectedSymbol: string, enabled: boolean = true): TerminalState {
  const ctx = getContext(tabId);

  useEffect(() => {
    if (!enabled) {
      ctx.ws.close();
      return;
    }
    ctx.expectedSymbol = normalizeSymbol(expectedSymbol);
    ctx.ws.setExpectedSymbol(ctx.expectedSymbol);
    ensureContextInitialized(ctx);
    return () => {
      // Keep backend session alive, but stop inactive tab client streaming.
      ctx.ws.close();
    };
  }, [ctx, expectedSymbol, enabled]);

  return useSyncExternalStore(
    (listener) => ctx.store.subscribe(listener),
    () => ctx.store.getSnapshot(),
    () => ctx.store.getSnapshot()
  );
}
