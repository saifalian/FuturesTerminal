import { useEffect, useMemo, useRef, useState } from "react";
import { closeTerminalTabClient, initializeTerminalTab } from "./hooks/useTerminalStream";
import LogsPage from "./pages/LogsPage";
import ReplayPage from "./pages/ReplayPage";
import SettingsPage from "./pages/SettingsPage";
import type { SettingsView } from "./pages/SettingsPage";
import TerminalPage from "./pages/TerminalPage";
import { SourceCoverageSelectionResponse, SourceCoverageUpdate, SymbolResolutionMessage } from "./types/market";

type RouteKey = "terminal" | "replay" | "settings" | "logs";
export type WorkloadControls = {
  live_streaming_enabled: boolean;
  replay_enabled: boolean;
  training_enabled: boolean;
  bots_enabled: boolean;
  backfill_enabled: boolean;
  training_ram_budget_gb: number | null;
  training_prefetch_enabled: boolean;
  training_chunk_group_size: "auto" | 1 | 2 | 3 | 5 | 8 | 10;
  training_process_recycle_enabled: boolean;
  training_worker_groups_before_restart: number;
  training_worker_memory_cap_gb: number | null;
  keep_training_shards: boolean;
};
export type RecordingMode = "lightweight" | "full_fidelity";
export type PairRecordingPreferences = Record<string, boolean>;
export type FeedCadence = { on_screen_ms: number; background_ms: number; render_ms: number };
type FeedCadenceByTab = Record<string, FeedCadence>;

type TerminalWorkspaceTab = {
  tab_id: string;
  symbol: string;
  title: string;
  created_at: string;
  last_active_at: string;
  terminal_mode: "live" | "replay";
  replay_session_id?: string;
};

type StoredTabsPayload = {
  tabs?: TerminalWorkspaceTab[];
  active_tab_id?: string;
};

const TAB_STORAGE_KEY = "futures_terminal_tabs_v1";
const WORKLOAD_CONTROLS_KEY = "futures_workload_controls_v1";
const RECORDING_PREFS_KEY = "futures_recording_prefs_v1";
const RECORDING_MODE_KEY = "futures_recording_mode_v1";
const ROUTE_SESSION_KEY = "futures_route_session_v1";
const SETTINGS_VIEW_SESSION_KEY = "futures_settings_view_session_v1";
const FEED_CADENCE_KEY = "futures_feed_cadence_v1";
const FEED_DELAY_MIN_MS = 100;
const FEED_DELAY_MAX_MS = 20_000;
const FEED_DELAY_DEFAULT_ON_SCREEN_MS = 500;
const FEED_DELAY_DEFAULT_BACKGROUND_MS = 2_000;
const FEED_DELAY_DEFAULT_RENDER_MS = 500;

const DEFAULT_WORKLOAD_CONTROLS: WorkloadControls = {
  live_streaming_enabled: true,
  replay_enabled: true,
  training_enabled: true,
  bots_enabled: true,
  backfill_enabled: true,
  training_ram_budget_gb: null,
  training_prefetch_enabled: false,
  training_chunk_group_size: "auto",
  training_process_recycle_enabled: true,
  training_worker_groups_before_restart: 1,
  training_worker_memory_cap_gb: null,
  keep_training_shards: false,
};

function loadWorkloadControls(): WorkloadControls {
  try {
    const raw = window.localStorage.getItem(WORKLOAD_CONTROLS_KEY);
    if (!raw) return { ...DEFAULT_WORKLOAD_CONTROLS };
    const parsed = JSON.parse(raw) as Partial<WorkloadControls>;
    return {
      live_streaming_enabled: parsed.live_streaming_enabled !== false,
      replay_enabled: parsed.replay_enabled !== false,
      training_enabled: parsed.training_enabled !== false,
      bots_enabled: parsed.bots_enabled !== false,
      backfill_enabled: parsed.backfill_enabled !== false,
      training_ram_budget_gb:
        typeof parsed.training_ram_budget_gb === "number" && Number.isFinite(parsed.training_ram_budget_gb)
          ? parsed.training_ram_budget_gb
          : null,
      training_prefetch_enabled: parsed.training_prefetch_enabled === true,
      training_chunk_group_size:
        parsed.training_chunk_group_size === 1 ||
        parsed.training_chunk_group_size === 2 ||
        parsed.training_chunk_group_size === 3 ||
        parsed.training_chunk_group_size === 5 ||
        parsed.training_chunk_group_size === 8 ||
        parsed.training_chunk_group_size === 10
          ? parsed.training_chunk_group_size
          : "auto",
      training_process_recycle_enabled: parsed.training_process_recycle_enabled !== false,
      training_worker_groups_before_restart:
        typeof parsed.training_worker_groups_before_restart === "number" && Number.isFinite(parsed.training_worker_groups_before_restart)
          ? Math.max(1, Math.min(50, Math.round(parsed.training_worker_groups_before_restart)))
          : 1,
      training_worker_memory_cap_gb:
        typeof parsed.training_worker_memory_cap_gb === "number" && Number.isFinite(parsed.training_worker_memory_cap_gb)
          ? parsed.training_worker_memory_cap_gb
          : null,
      keep_training_shards: parsed.keep_training_shards === true,
    };
  } catch {
    return { ...DEFAULT_WORKLOAD_CONTROLS };
  }
}

function loadPairRecordingPreferences(): PairRecordingPreferences {
  try {
    const raw = window.localStorage.getItem(RECORDING_PREFS_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const out: PairRecordingPreferences = {};
    for (const [key, value] of Object.entries(parsed || {})) {
      const symbol = normalizeSymbol(key);
      if (!symbol) continue;
      out[symbol] = Boolean(value);
    }
    return out;
  } catch {
    return {};
  }
}

function normalizeRecordingMode(value: unknown): RecordingMode {
  return String(value || "").trim().toLowerCase() === "full_fidelity" ? "full_fidelity" : "lightweight";
}

function loadRecordingMode(): RecordingMode {
  try {
    const raw = window.localStorage.getItem(RECORDING_MODE_KEY);
    return normalizeRecordingMode(raw);
  } catch {
    return "lightweight";
  }
}

function clampFeedDelayMs(value: number | string | null | undefined, fallback: number): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  const rounded = Math.round(numeric);
  return Math.max(FEED_DELAY_MIN_MS, Math.min(FEED_DELAY_MAX_MS, rounded));
}

function defaultFeedCadence(): FeedCadence {
  return {
    on_screen_ms: FEED_DELAY_DEFAULT_ON_SCREEN_MS,
    background_ms: FEED_DELAY_DEFAULT_BACKGROUND_MS,
    render_ms: FEED_DELAY_DEFAULT_RENDER_MS,
  };
}

function loadFeedCadenceByTab(): FeedCadenceByTab {
  try {
    const raw = window.localStorage.getItem(FEED_CADENCE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as Record<string, Partial<FeedCadence>>;
    const out: FeedCadenceByTab = {};
    for (const [tabId, value] of Object.entries(parsed || {})) {
      const normalizedTab = String(tabId || "").trim().toLowerCase();
      if (!normalizedTab) continue;
      out[normalizedTab] = {
        on_screen_ms: clampFeedDelayMs(value?.on_screen_ms, FEED_DELAY_DEFAULT_ON_SCREEN_MS),
        background_ms: clampFeedDelayMs(value?.background_ms, FEED_DELAY_DEFAULT_BACKGROUND_MS),
        render_ms: clampFeedDelayMs(value?.render_ms, FEED_DELAY_DEFAULT_RENDER_MS),
      };
    }
    return out;
  } catch {
    return {};
  }
}

function formatFeedDelayLabel(ms: number): string {
  if (ms < 1000) return `${ms} ms`;
  const sec = ms / 1000;
  return `${sec.toFixed(sec % 1 === 0 ? 0 : 1)} s`;
}

function loadRouteSession(): RouteKey {
  try {
    const raw = window.sessionStorage.getItem(ROUTE_SESSION_KEY);
    return raw === "settings" || raw === "logs" || raw === "replay" ? raw : "terminal";
  } catch {
    return "terminal";
  }
}

function loadSettingsViewSession(): SettingsView {
  try {
    const raw = window.sessionStorage.getItem(SETTINGS_VIEW_SESSION_KEY);
    return raw === "workspace_presets" || raw === "trainer_bot" || raw === "service_settings" || raw === "storage_management"
      ? raw
      : "hub";
  } catch {
    return "hub";
  }
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeSymbol(value: string | undefined | null) {
  const normalized = (value ?? "").trim().toUpperCase();
  return normalized || "BTCUSDT";
}

function createTabId() {
  return `tab_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function defaultTab(symbol: string = "BTCUSDT"): TerminalWorkspaceTab {
  const ts = nowIso();
  const normalized = normalizeSymbol(symbol);
  return {
    tab_id: "default",
    symbol: normalized,
    title: normalized,
    created_at: ts,
    last_active_at: ts,
    terminal_mode: "live",
    replay_session_id: "",
  };
}

function loadStoredTabs(): { tabs: TerminalWorkspaceTab[]; activeTabId: string } {
  try {
    const raw = window.localStorage.getItem(TAB_STORAGE_KEY);
    if (!raw) {
      const tab = defaultTab();
      return { tabs: [tab], activeTabId: tab.tab_id };
    }
    const parsed = JSON.parse(raw) as StoredTabsPayload;
    const incoming = Array.isArray(parsed.tabs) ? parsed.tabs : [];
    const seen = new Set<string>();
    const sanitized = incoming
      .map((item): TerminalWorkspaceTab => {
        const rawId = String(item.tab_id || "").trim();
        let tabId = rawId || createTabId();
        if (seen.has(tabId)) {
          tabId = createTabId();
        }
        seen.add(tabId);
        const terminalMode: "live" | "replay" = item.terminal_mode === "replay" ? "replay" : "live";
        return {
          tab_id: tabId,
          symbol: normalizeSymbol(item.symbol),
          title: normalizeSymbol(item.title || item.symbol),
          created_at: String(item.created_at || nowIso()),
          last_active_at: String(item.last_active_at || nowIso()),
          terminal_mode: terminalMode,
          replay_session_id: String(item.replay_session_id || ""),
        };
      })
      .filter((item) => item.tab_id.length > 0);
    const tabs = sanitized.length > 0 ? sanitized : [defaultTab()];
    const activeCandidate = String(parsed.active_tab_id || "").trim();
    const activeTabId = tabs.some((tab) => tab.tab_id === activeCandidate) ? activeCandidate : tabs[0].tab_id;
    return { tabs, activeTabId };
  } catch {
    const tab = defaultTab();
    return { tabs: [tab], activeTabId: tab.tab_id };
  }
}

export default function App() {
  const storedTabs = useMemo(() => loadStoredTabs(), []);
  const initialWorkloadControls = useMemo(() => loadWorkloadControls(), []);
  const initialRecordingPrefs = useMemo(() => loadPairRecordingPreferences(), []);
  const initialRecordingMode = useMemo(() => loadRecordingMode(), []);
  const initialFeedCadenceByTab = useMemo(() => loadFeedCadenceByTab(), []);
  const [route, setRoute] = useState<RouteKey>(() => loadRouteSession());
  const [watchlist, setWatchlist] = useState<string[]>(["BTCUSDT"]);
  const [allSymbols, setAllSymbols] = useState<string[]>(["BTCUSDT"]);
  const [terminalTabs, setTerminalTabs] = useState<TerminalWorkspaceTab[]>(storedTabs.tabs);
  const [activeTerminalTabId, setActiveTerminalTabId] = useState<string>(storedTabs.activeTabId);
  const [selectedSymbol, setSelectedSymbol] = useState<string>(storedTabs.tabs[0]?.symbol ?? "BTCUSDT");
  const [switching, setSwitching] = useState<boolean>(false);
  const [switchError, setSwitchError] = useState<string>("");
  const [showSuggestions, setShowSuggestions] = useState<boolean>(false);
  const [showCoverageInfo, setShowCoverageInfo] = useState<boolean>(false);
  const [sourceCoverage, setSourceCoverage] = useState<SourceCoverageUpdate | null>(null);
  const [symbolResolution, setSymbolResolution] = useState<SymbolResolutionMessage | null>(null);
  const [coverageApplying, setCoverageApplying] = useState<boolean>(false);
  const [coverageDraftEnabledIds, setCoverageDraftEnabledIds] = useState<Set<string> | null>(null);
  const [coverageDraftFeedCadence, setCoverageDraftFeedCadence] = useState<FeedCadence | null>(null);
  const [coverageDraftError, setCoverageDraftError] = useState<string>("");
  const [workloadControls, setWorkloadControls] = useState<WorkloadControls>(initialWorkloadControls);
  const workloadHydratedRef = useRef(false);
  const [pairRecordingPrefs, setPairRecordingPrefs] = useState<PairRecordingPreferences>(initialRecordingPrefs);
  const [recordingMode, setRecordingMode] = useState<RecordingMode>(initialRecordingMode);
  const [feedCadenceByTab, setFeedCadenceByTab] = useState<FeedCadenceByTab>(initialFeedCadenceByTab);
  const [settingsView, setSettingsView] = useState<SettingsView>(() => loadSettingsViewSession());
  const coverageFetchInFlight = useRef(false);
  const hydratedTabsRef = useRef<Set<string>>(new Set());

  const activeTab = useMemo(
    () => terminalTabs.find((tab) => tab.tab_id === activeTerminalTabId) ?? terminalTabs[0] ?? defaultTab(),
    [terminalTabs, activeTerminalTabId]
  );
  const activeSymbol = activeTab.symbol;
  const activeTerminalMode = activeTab.terminal_mode ?? "live";
  const activeTabFeedCadence = feedCadenceByTab[activeTab.tab_id] ?? defaultFeedCadence();
  const activeDraftFeedCadence = coverageDraftFeedCadence ?? activeTabFeedCadence;

  useEffect(() => {
    window.localStorage.setItem(WORKLOAD_CONTROLS_KEY, JSON.stringify(workloadControls));
  }, [workloadControls]);

  useEffect(() => {
    let cancelled = false;
    async function hydrateWorkloadControls() {
      try {
        const response = await fetch("http://127.0.0.1:8000/settings/workload-controls", { cache: "no-store" });
        if (!response.ok) return;
        const data = (await response.json()) as Partial<WorkloadControls>;
        if (cancelled) return;
        setWorkloadControls((prev) => ({
          live_streaming_enabled: data.live_streaming_enabled !== false,
          replay_enabled: data.replay_enabled !== false,
          training_enabled: data.training_enabled !== false,
          bots_enabled: data.bots_enabled !== false,
          backfill_enabled: data.backfill_enabled !== false,
          training_ram_budget_gb:
            typeof data.training_ram_budget_gb === "number" && Number.isFinite(data.training_ram_budget_gb)
              ? data.training_ram_budget_gb
              : null,
          training_prefetch_enabled: data.training_prefetch_enabled === true,
          training_chunk_group_size:
            data.training_chunk_group_size === 1 ||
            data.training_chunk_group_size === 2 ||
            data.training_chunk_group_size === 3 ||
            data.training_chunk_group_size === 5 ||
            data.training_chunk_group_size === 8 ||
            data.training_chunk_group_size === 10
              ? data.training_chunk_group_size
              : "auto",
          training_process_recycle_enabled: data.training_process_recycle_enabled !== false,
          training_worker_groups_before_restart:
            typeof data.training_worker_groups_before_restart === "number" && Number.isFinite(data.training_worker_groups_before_restart)
              ? Math.max(1, Math.min(50, Math.round(data.training_worker_groups_before_restart)))
              : 1,
          training_worker_memory_cap_gb:
            typeof data.training_worker_memory_cap_gb === "number" && Number.isFinite(data.training_worker_memory_cap_gb)
              ? data.training_worker_memory_cap_gb
              : null,
          keep_training_shards: data.keep_training_shards === true,
        }));
      } catch {
        // keep local fallback
      } finally {
        workloadHydratedRef.current = true;
      }
    }
    void hydrateWorkloadControls();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    window.localStorage.setItem(RECORDING_PREFS_KEY, JSON.stringify(pairRecordingPrefs));
  }, [pairRecordingPrefs]);

  useEffect(() => {
    window.localStorage.setItem(RECORDING_MODE_KEY, recordingMode);
  }, [recordingMode]);

  useEffect(() => {
    window.localStorage.setItem(FEED_CADENCE_KEY, JSON.stringify(feedCadenceByTab));
  }, [feedCadenceByTab]);

  useEffect(() => {
    try {
      window.sessionStorage.setItem(ROUTE_SESSION_KEY, route);
    } catch {
      // ignore session storage errors
    }
  }, [route]);

  useEffect(() => {
    try {
      window.sessionStorage.setItem(SETTINGS_VIEW_SESSION_KEY, settingsView);
    } catch {
      // ignore session storage errors
    }
  }, [settingsView]);

  useEffect(() => {
    let cancelled = false;
    async function hydrateRecordingPrefs() {
      try {
        const response = await fetch("http://127.0.0.1:8000/settings/recording-preferences");
        if (!response.ok) return;
        const data = (await response.json()) as { pairs?: Record<string, boolean> };
        const pairs = data.pairs ?? {};
        if (cancelled) return;
        setPairRecordingPrefs((prev) => ({ ...prev, ...pairs }));
      } catch {
        // keep local defaults when backend is not reachable
      }
    }
    void hydrateRecordingPrefs();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function hydrateRecordingMode() {
      try {
        const response = await fetch("http://127.0.0.1:8000/settings/recording-mode", { cache: "no-store" });
        if (!response.ok) return;
        const data = (await response.json()) as { mode?: string };
        if (cancelled) return;
        setRecordingMode(normalizeRecordingMode(data.mode));
      } catch {
        // keep local fallback
      }
    }
    void hydrateRecordingMode();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const pairEnabled = Boolean(pairRecordingPrefs[activeSymbol]);
    const enabled = workloadControls.live_streaming_enabled && pairEnabled;
    void fetch("http://127.0.0.1:8000/settings/recording-state", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        active_pair_symbol: activeSymbol,
        enabled,
      }),
    }).catch(() => {});
  }, [activeSymbol, pairRecordingPrefs, workloadControls.live_streaming_enabled, activeTerminalTabId]);

  async function handlePairRecordingToggle(pairSymbol: string, enabled: boolean) {
    const normalized = normalizeSymbol(pairSymbol);
    setPairRecordingPrefs((prev) => ({ ...prev, [normalized]: enabled }));
    try {
      await fetch("http://127.0.0.1:8000/settings/recording-preferences", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          pair_symbol: normalized,
          enabled,
        }),
      });
    } catch {
      // local state remains as fallback when backend is unavailable
    }
  }

  async function handleWorkloadControlsChange(next: WorkloadControls) {
    setWorkloadControls(next);
    if (!workloadHydratedRef.current) return;
    try {
      await fetch("http://127.0.0.1:8000/settings/workload-controls", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(next),
      });
    } catch {
      // keep local state when backend is unreachable
    }
  }

  async function handleRecordingModeChange(nextMode: RecordingMode) {
    setRecordingMode(nextMode);
    try {
      await fetch("http://127.0.0.1:8000/settings/recording-mode", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: nextMode }),
      });
    } catch {
      // local state remains as fallback
    }
  }

  useEffect(() => {
    if (workloadControls.live_streaming_enabled) return;
    if (activeTerminalMode === "live" && workloadControls.replay_enabled) {
      setActiveTabMode("replay");
    }
    closeTerminalTabClient(activeTerminalTabId);
  }, [workloadControls.live_streaming_enabled, workloadControls.replay_enabled, activeTerminalMode, activeTerminalTabId]);

  useEffect(() => {
    if (workloadControls.replay_enabled) return;
    if (activeTerminalMode === "replay" && workloadControls.live_streaming_enabled) {
      setActiveTabMode("live");
    }
  }, [workloadControls.replay_enabled, workloadControls.live_streaming_enabled, activeTerminalMode]);

  function formatCoverageReason(reason: string) {
    switch (reason) {
      case "ready":
        return "Ready";
      case "pair_not_supported":
        return "Pair not supported";
      case "adapter_not_implemented":
        return "Coming soon";
      case "poll_error":
        return "API blocked / temporary error";
      case "no_depth_data":
        return "No depth data";
      case "adapter_unavailable":
        return "Adapter unavailable";
      case "client_init_failed":
        return "Client init failed";
      case "disabled_by_user":
        return "Disabled";
      default:
        return reason.replaceAll("_", " ");
    }
  }

  function featureState(item: NonNullable<SourceCoverageUpdate["exchanges"]>[number], feature: "book" | "trades" | "candles" | "mark_funding" | "liquidations") {
    if (!item.enabled_by_user) return "off";
    if (!item.supports_pair) return "na";
    if (!item.features[feature]) return item.connected ? "ready" : "idle";
    return item.connected ? "live" : "ready";
  }

  function featureStateLabel(state: "off" | "na" | "waiting" | "idle" | "live" | "ready") {
    switch (state) {
      case "off":
        return "Off";
      case "na":
        return "N/A";
      case "waiting":
        return "Waiting";
      case "idle":
        return "Idle";
      case "ready":
        return "Ready";
      case "live":
        return "Live";
      default:
        return "-";
    }
  }

  const filteredSymbols = useMemo(() => {
    const typed = selectedSymbol.trim().toUpperCase();
    if (!typed) return allSymbols.slice(0, 20);

    const starts = allSymbols.filter((symbol) => symbol.startsWith(typed));
    const contains = allSymbols.filter((symbol) => !symbol.startsWith(typed) && symbol.includes(typed));
    return [...starts, ...contains].slice(0, 25);
  }, [allSymbols, selectedSymbol]);

  async function refreshSymbols(tabId: string) {
    try {
      const query = new URLSearchParams({ tab_id: tabId });
      const response = await fetch(`http://127.0.0.1:8000/settings/symbols?${query.toString()}`);
      const data = (await response.json()) as { watchlist?: string[]; all_symbols?: string[] };
      const list = data.watchlist && data.watchlist.length > 0 ? data.watchlist : ["BTCUSDT"];
      const fullList = data.all_symbols && data.all_symbols.length > 0 ? data.all_symbols : list;
      setWatchlist(list);
      setAllSymbols(fullList);
    } catch {
      // leave defaults if backend is not reachable yet
    }
  }

  async function refreshCoverage(tabId: string) {
    if (!workloadControls.live_streaming_enabled) return;
    if (coverageFetchInFlight.current) return;
    coverageFetchInFlight.current = true;
    try {
      const query = new URLSearchParams({ tab_id: tabId });
      const response = await fetch(`http://127.0.0.1:8000/terminal/source-coverage?${query.toString()}`);
      const data = (await response.json()) as Partial<SourceCoverageUpdate>;
      if (data.type === "source_coverage_update") {
        setSourceCoverage(data as SourceCoverageUpdate);
      }
    } catch {
      // ignore coverage fetch errors
    } finally {
      coverageFetchInFlight.current = false;
    }
  }

  async function refreshFeedCadence(tabId: string) {
    if (!workloadControls.live_streaming_enabled) return;
    try {
      const query = new URLSearchParams({ tab_id: tabId });
      const response = await fetch(`http://127.0.0.1:8000/terminal/feed-cadence?${query.toString()}`, {
        cache: "no-store",
      });
      if (!response.ok) return;
      const data = (await response.json()) as Partial<FeedCadence>;
      setFeedCadenceByTab((prev) => ({
        ...prev,
        [tabId]: {
          on_screen_ms: clampFeedDelayMs(data.on_screen_ms, FEED_DELAY_DEFAULT_ON_SCREEN_MS),
          background_ms: clampFeedDelayMs(data.background_ms, FEED_DELAY_DEFAULT_BACKGROUND_MS),
          render_ms: clampFeedDelayMs(data.render_ms, FEED_DELAY_DEFAULT_RENDER_MS),
        },
      }));
    } catch {
      // keep local fallback
    }
  }

  async function saveFeedCadence(tabId: string, cadence: FeedCadence) {
    try {
      await fetch("http://127.0.0.1:8000/terminal/feed-cadence", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tab_id: tabId,
          on_screen_ms: clampFeedDelayMs(cadence.on_screen_ms, FEED_DELAY_DEFAULT_ON_SCREEN_MS),
          background_ms: clampFeedDelayMs(cadence.background_ms, FEED_DELAY_DEFAULT_BACKGROUND_MS),
          render_ms: clampFeedDelayMs(cadence.render_ms, FEED_DELAY_DEFAULT_RENDER_MS),
        }),
      });
    } catch {
      // keep local fallback when backend unavailable
    }
  }

  function handleFeedCadenceChange(kind: "on_screen_ms" | "background_ms" | "render_ms", value: number) {
    const nextValue = clampFeedDelayMs(
      value,
      kind === "on_screen_ms"
        ? FEED_DELAY_DEFAULT_ON_SCREEN_MS
        : kind === "background_ms"
          ? FEED_DELAY_DEFAULT_BACKGROUND_MS
          : FEED_DELAY_DEFAULT_RENDER_MS
    );
    setCoverageDraftFeedCadence((prev) => {
      const current = prev ?? activeTabFeedCadence;
      return { ...current, [kind]: nextValue };
    });
  }

  useEffect(() => {
    const payload: StoredTabsPayload = {
      tabs: terminalTabs,
      active_tab_id: activeTerminalTabId,
    };
    window.localStorage.setItem(TAB_STORAGE_KEY, JSON.stringify(payload));
  }, [terminalTabs, activeTerminalTabId]);

  useEffect(() => {
    setSelectedSymbol(activeSymbol);
    setSymbolResolution(null);
    setSourceCoverage(null);
    setCoverageDraftEnabledIds(null);
    setCoverageDraftError("");
    void refreshSymbols(activeTab.tab_id);
    if (workloadControls.live_streaming_enabled) {
      void refreshCoverage(activeTab.tab_id);
    }
  }, [activeSymbol, activeTab.tab_id, workloadControls.live_streaming_enabled]);

  useEffect(() => {
    for (const tab of terminalTabs) {
      if (hydratedTabsRef.current.has(tab.tab_id)) continue;
      hydratedTabsRef.current.add(tab.tab_id);
      void fetch("http://127.0.0.1:8000/settings/symbol", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tab_id: tab.tab_id, symbol: tab.symbol }),
      }).catch(() => {});
    }
  }, [terminalTabs]);

  useEffect(() => {
    if (!workloadControls.live_streaming_enabled) {
      closeTerminalTabClient(activeTerminalTabId);
      return;
    }
    void initializeTerminalTab(activeTerminalTabId);
  }, [activeTerminalTabId, workloadControls.live_streaming_enabled]);

  useEffect(() => {
    if (terminalTabs.some((tab) => tab.tab_id === activeTerminalTabId)) return;
    setActiveTerminalTabId(terminalTabs[0]?.tab_id ?? "default");
  }, [terminalTabs, activeTerminalTabId]);

  useEffect(() => {
    setFeedCadenceByTab((prev) => {
      let changed = false;
      const next: FeedCadenceByTab = { ...prev };
      const liveTabIds = new Set(terminalTabs.map((tab) => tab.tab_id));
      for (const tab of terminalTabs) {
        if (!next[tab.tab_id]) {
          next[tab.tab_id] = defaultFeedCadence();
          changed = true;
        }
      }
      for (const existingTabId of Object.keys(next)) {
        if (!liveTabIds.has(existingTabId)) {
          delete next[existingTabId];
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [terminalTabs]);

  useEffect(() => {
    if (!showCoverageInfo || !workloadControls.live_streaming_enabled) return;
    void refreshFeedCadence(activeTab.tab_id);
    const timer = window.setInterval(() => {
      void refreshCoverage(activeTab.tab_id);
    }, 4000);
    return () => window.clearInterval(timer);
  }, [showCoverageInfo, activeTab.tab_id, workloadControls.live_streaming_enabled]);

  useEffect(() => {
    if (!showCoverageInfo) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previous;
    };
  }, [showCoverageInfo]);

  useEffect(() => {
    if (!showCoverageInfo || !sourceCoverage || coverageDraftEnabledIds !== null) return;
    setCoverageDraftEnabledIds(new Set(sourceCoverage.enabled_exchange_ids ?? []));
  }, [showCoverageInfo, sourceCoverage, coverageDraftEnabledIds]);

  async function handleSwitchPair() {
    const symbol = selectedSymbol.trim().toUpperCase();
    if (!symbol || switching || symbol === activeSymbol) return;
    setSwitching(true);
    setSwitchError("");
    try {
      const response = await fetch("http://127.0.0.1:8000/settings/symbol", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tab_id: activeTab.tab_id, symbol }),
      });
      const data = (await response.json()) as {
        ok?: boolean;
        active_symbol?: string;
        error?: string;
        source_coverage?: SourceCoverageUpdate;
        symbol_resolution?: SymbolResolutionMessage;
      };
      if (data.ok && data.active_symbol) {
        const nextSymbol = normalizeSymbol(data.active_symbol);
        setTerminalTabs((prev) =>
          prev.map((tab) =>
            tab.tab_id === activeTab.tab_id
              ? {
                  ...tab,
                  symbol: nextSymbol,
                  title: nextSymbol,
                  last_active_at: nowIso(),
                }
              : tab
          )
        );
        setSelectedSymbol(nextSymbol);
        if (data.source_coverage) setSourceCoverage(data.source_coverage);
        if (data.symbol_resolution) setSymbolResolution(data.symbol_resolution);
      } else if (data.error) {
        setSwitchError(data.error);
      }
    } finally {
      setSwitching(false);
    }
  }

  function closeCoverageModal() {
    setShowCoverageInfo(false);
    setCoverageDraftEnabledIds(null);
    setCoverageDraftFeedCadence(null);
    setCoverageDraftError("");
    setCoverageApplying(false);
  }

  function openCoverageModal() {
    setCoverageDraftEnabledIds(null);
    setCoverageDraftFeedCadence(activeTabFeedCadence);
    setCoverageDraftError("");
    setCoverageApplying(false);
    setShowCoverageInfo(true);
    void refreshCoverage(activeTab.tab_id);
    void refreshFeedCadence(activeTab.tab_id);
  }

  function markTabActive(tabId: string) {
    setActiveTerminalTabId(tabId);
    setTerminalTabs((prev) =>
      prev.map((tab) => (tab.tab_id === tabId ? { ...tab, last_active_at: nowIso() } : tab))
    );
  }

  function createNewTab() {
    const baseSymbol = normalizeSymbol(activeSymbol);
    const ts = nowIso();
    const newTab: TerminalWorkspaceTab = {
      tab_id: createTabId(),
      symbol: baseSymbol,
      title: baseSymbol,
      created_at: ts,
      last_active_at: ts,
      terminal_mode: "live",
      replay_session_id: "",
    };
    setTerminalTabs((prev) => [...prev, newTab]);
    setActiveTerminalTabId(newTab.tab_id);
    setRoute("terminal");
    closeCoverageModal();
  }

  function setActiveTabMode(mode: "live" | "replay") {
    const currentReplayId = activeTab.replay_session_id || "";
    if (mode === "live" && currentReplayId) {
      void fetch(`http://127.0.0.1:8000/ml/replay/${currentReplayId}/control`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "close" }),
      }).catch(() => {});
    }
    setTerminalTabs((prev) =>
      prev.map((tab) =>
        tab.tab_id === activeTab.tab_id
          ? {
              ...tab,
              terminal_mode: mode,
              replay_session_id: mode === "live" ? "" : tab.replay_session_id,
            }
          : tab
      )
    );
  }

  function setActiveTabReplaySession(replaySessionId: string) {
    setTerminalTabs((prev) =>
      prev.map((tab) =>
        tab.tab_id === activeTab.tab_id
          ? {
              ...tab,
              replay_session_id: replaySessionId,
            }
          : tab
      )
    );
  }

  function closeTab(tabId: string) {
    if (terminalTabs.length <= 1) return;
    const index = terminalTabs.findIndex((tab) => tab.tab_id === tabId);
    if (index === -1) return;
    const nextTabs = terminalTabs.filter((tab) => tab.tab_id !== tabId);
    const closing = terminalTabs.find((tab) => tab.tab_id === tabId);
    const replayId = closing?.replay_session_id || "";
    if (replayId) {
      void fetch(`http://127.0.0.1:8000/ml/replay/${replayId}/control`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "close" }),
      }).catch(() => {});
    }
    const nextActive =
      activeTerminalTabId === tabId
        ? (nextTabs[Math.max(0, index - 1)]?.tab_id ?? nextTabs[0]?.tab_id ?? "default")
        : activeTerminalTabId;
    setTerminalTabs(nextTabs);
    setActiveTerminalTabId(nextActive);
    hydratedTabsRef.current.delete(tabId);
    closeTerminalTabClient(tabId);
    closeCoverageModal();
    const query = new URLSearchParams({ tab_id: tabId });
    void fetch(`http://127.0.0.1:8000/terminal/tab?${query.toString()}`, {
      method: "DELETE",
    }).catch(() => {});
  }

  useEffect(() => {
    if (!showCoverageInfo) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        closeCoverageModal();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [showCoverageInfo]);

  function handleCoverageDraftToggle(exchangeId: string, checked: boolean) {
    if (coverageApplying) return;
    setCoverageDraftError("");
    setCoverageDraftEnabledIds((prev) => {
      const base = prev ?? new Set(sourceCoverage?.enabled_exchange_ids ?? []);
      const next = new Set(base);
      if (checked) {
        next.add(exchangeId);
      } else {
        next.delete(exchangeId);
      }
      return next;
    });
  }

  function handleCoverageCheckAll() {
    if (coverageApplying || !sourceCoverage) return;
    setCoverageDraftError("");
    setCoverageDraftEnabledIds(new Set(sourceCoverage.exchanges.map((item) => item.exchange_id)));
  }

  function handleCoverageUncheckAll() {
    if (coverageApplying) return;
    setCoverageDraftError("");
    setCoverageDraftEnabledIds(new Set());
  }

  const coverageDraftDirty = useMemo(() => {
    if (!sourceCoverage || !coverageDraftEnabledIds) return false;
    const currentEnabled = sourceCoverage.enabled_exchange_ids ?? [];
    if (currentEnabled.length !== coverageDraftEnabledIds.size) return true;
    for (const exchangeId of currentEnabled) {
      if (!coverageDraftEnabledIds.has(exchangeId)) return true;
    }
    return false;
  }, [sourceCoverage, coverageDraftEnabledIds]);

  const coverageCadenceDirty = useMemo(() => {
    if (!coverageDraftFeedCadence) return false;
    return (
      coverageDraftFeedCadence.on_screen_ms !== activeTabFeedCadence.on_screen_ms
      || coverageDraftFeedCadence.background_ms !== activeTabFeedCadence.background_ms
      || coverageDraftFeedCadence.render_ms !== activeTabFeedCadence.render_ms
    );
  }, [coverageDraftFeedCadence, activeTabFeedCadence]);

  async function handleApplyCoverageSelection() {
    if (!coverageDraftEnabledIds && !coverageCadenceDirty) return;
    const nextEnabled = new Set(coverageDraftEnabledIds ?? []);
    const nextCadence = coverageDraftFeedCadence ?? activeTabFeedCadence;
    setCoverageApplying(true);
    setCoverageDraftError("");
    try {
      if (coverageDraftEnabledIds) {
        const response = await fetch("http://127.0.0.1:8000/terminal/source-coverage/selection", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ tab_id: activeTab.tab_id, enabled_exchange_ids: [...nextEnabled] }),
        });
        const data = (await response.json()) as Partial<SourceCoverageSelectionResponse>;
        if (data.ok && data.source_coverage?.type === "source_coverage_update") {
          setSourceCoverage(data.source_coverage);
          setCoverageDraftEnabledIds(new Set(data.source_coverage.enabled_exchange_ids ?? []));
        } else {
          setCoverageDraftError("Apply failed. Please try again.");
          return;
        }
      }
      if (coverageCadenceDirty) {
        await saveFeedCadence(activeTab.tab_id, nextCadence);
        setFeedCadenceByTab((prev) => ({ ...prev, [activeTab.tab_id]: nextCadence }));
        setCoverageDraftFeedCadence(nextCadence);
      }
    } catch {
      setCoverageDraftError("Apply failed. Please check backend connection.");
    } finally {
      setCoverageApplying(false);
    }
  }

  return (
    <div className={`app-shell ${route === "terminal" ? "with-terminal-tabs" : ""}`}>
      <header className="top-nav">
        <div className="top-nav-left">
          <div className="brand">Futures Terminal</div>
        </div>
        <div className="top-nav-center">
          <div className="pair-label">Active Pair: {activeSymbol}</div>
          <div className="pair-search-box">
            <input
              value={selectedSymbol}
              onChange={(e) => {
                setSelectedSymbol(e.target.value.toUpperCase());
                setShowSuggestions(true);
              }}
              onFocus={() => setShowSuggestions(true)}
              onBlur={() => setTimeout(() => setShowSuggestions(false), 120)}
              placeholder="Type pair e.g. ETHUSDT"
            />
            {showSuggestions && filteredSymbols.length > 0 ? (
              <div className="pair-suggestions">
                {filteredSymbols.map((pair) => (
                  <button
                    key={pair}
                    type="button"
                    className="pair-suggestion-item"
                    onMouseDown={(event) => {
                      event.preventDefault();
                      setSelectedSymbol(pair);
                      setShowSuggestions(false);
                    }}
                  >
                    {pair}
                  </button>
                ))}
              </div>
            ) : null}
          </div>
          <button onClick={handleSwitchPair} disabled={switching || selectedSymbol === activeSymbol}>
            {switching ? "Switching..." : "Switch Pair"}
          </button>
          <button
            title="Exchange coverage info"
            onClick={() => {
              if (showCoverageInfo) {
                closeCoverageModal();
              } else {
                openCoverageModal();
              }
            }}
          >
            i
          </button>
          {switchError ? <div className="pair-error">{switchError}</div> : null}
        </div>
        <nav className="top-nav-right">
          <button onClick={() => setRoute("terminal")}>Terminal</button>
          <button onClick={() => setRoute("settings")}>Settings</button>
          <button onClick={() => setRoute("logs")}>Logs</button>
        </nav>
      </header>

      {route === "terminal" ? (
        <div className="terminal-tabs-row">
          <div className="terminal-tabs-scroll">
            {terminalTabs.map((tab) => {
              const active = tab.tab_id === activeTerminalTabId;
              return (
                <button
                  key={tab.tab_id}
                  type="button"
                  className={`terminal-tab ${active ? "active" : ""}`}
                  onClick={() => markTabActive(tab.tab_id)}
                >
                  <span className="terminal-tab-title">{tab.title}</span>
                  {terminalTabs.length > 1 ? (
                    <span
                      className="terminal-tab-close"
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        closeTab(tab.tab_id);
                      }}
                    >
                      x
                    </span>
                  ) : null}
                </button>
              );
            })}
            <button type="button" className="terminal-tab-add" onClick={createNewTab}>
              +
            </button>
          </div>
          <div className="watchlist-inline">Watchlist: {watchlist.join(", ")}</div>
          <div className="watchlist-inline" style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              className={activeTerminalMode === "live" ? "active" : ""}
              onClick={() => setActiveTabMode("live")}
              disabled={!workloadControls.live_streaming_enabled}
            >
              Live Terminal
            </button>
            <button
              type="button"
              className={activeTerminalMode === "replay" ? "active" : ""}
              onClick={() => setActiveTabMode("replay")}
              disabled={!workloadControls.replay_enabled}
            >
              Replay Terminal
            </button>
          </div>
        </div>
      ) : null}

      <main className="page-wrap">
        {route === "terminal" && activeTerminalMode === "live" ? (
          workloadControls.live_streaming_enabled ? (
            <TerminalPage key={`${activeTerminalTabId}-live`} tabId={activeTerminalTabId} expectedSymbol={activeSymbol} />
          ) : (
            <section className="panel">
              <div className="small-title">Live Terminal Disabled</div>
              <p>Enable Live Streaming in Settings to resume real-time market data.</p>
            </section>
          )
        ) : null}
        {route === "terminal" && activeTerminalMode === "replay" ? (
          workloadControls.replay_enabled ? (
            <ReplayPage
              key={`${activeTerminalTabId}-replay`}
              tabId={activeTerminalTabId}
              symbol={activeSymbol}
              replaySessionId={activeTab.replay_session_id || ""}
              onReplaySessionChange={setActiveTabReplaySession}
              activeTerminalMode={activeTerminalMode}
              onTerminalModeChange={setActiveTabMode}
            />
          ) : (
            <section className="panel">
              <div className="small-title">Replay Disabled</div>
              <p>Enable Replay in Settings to use replay sessions and chart replay controls.</p>
            </section>
          )
        ) : null}
        {route === "settings" ? (
          <div className="settings-route-wrap">
              <SettingsPage
                workloadControls={workloadControls}
                onWorkloadControlsChange={handleWorkloadControlsChange}
                recordingMode={recordingMode}
                onRecordingModeChange={handleRecordingModeChange}
                settingsView={settingsView}
                onSettingsViewChange={setSettingsView}
              />
          </div>
        ) : null}
        {route === "logs" ? <LogsPage /> : null}
      </main>

      {showCoverageInfo ? (
        <div className="coverage-modal-backdrop" onClick={closeCoverageModal}>
          <section
            className="coverage-modal panel"
            onClick={(event) => event.stopPropagation()}
            onWheelCapture={(event) => event.stopPropagation()}
          >
            <div className="small-title">Exchange Coverage ({sourceCoverage?.symbol ?? activeSymbol})</div>
            <div className="kv"><span>Total catalog</span><span>{sourceCoverage?.total_catalog_exchanges ?? 0}</span></div>
            <div className="kv"><span>Supported for pair</span><span>{sourceCoverage?.supported_exchanges ?? 0}</span></div>
            <div className="kv"><span>Enabled by user</span><span>{sourceCoverage?.enabled_exchanges ?? 0}</span></div>
            <div className="kv"><span>Live connected</span><span>{sourceCoverage?.connected_exchanges ?? 0}</span></div>
            <div className="small-title">Feature contributors</div>
            <div className="kv"><span>Book / spread</span><span>{sourceCoverage?.feature_contributors.book ?? 0}</span></div>
            <div className="kv"><span>Trades</span><span>{sourceCoverage?.feature_contributors.trades ?? 0}</span></div>
            <div className="kv"><span>Candles</span><span>{sourceCoverage?.feature_contributors.candles ?? 0}</span></div>
            <div className="kv"><span>Mark/Funding</span><span>{sourceCoverage?.feature_contributors.mark_funding ?? 0}</span></div>
            <div className="kv"><span>Liquidations</span><span>{sourceCoverage?.feature_contributors.liquidations ?? 0}</span></div>
            <div className="small-title">Resolution</div>
            <div className="kv"><span>Requested</span><span>{symbolResolution?.requested_symbol ?? "-"}</span></div>
            <div className="kv"><span>Normalized</span><span>{symbolResolution?.normalized_symbol ?? "-"}</span></div>
            {activeTerminalMode === "live" ? (
              <>
                <div className="small-title">Live Recording</div>
                <div className="coverage-recording-row">
                  <button
                    type="button"
                    className={`live-recording-toggle ${pairRecordingPrefs[activeSymbol] ? "on" : "off"}`}
                    onClick={() => void handlePairRecordingToggle(activeSymbol, !Boolean(pairRecordingPrefs[activeSymbol]))}
                  >
                    Record {pairRecordingPrefs[activeSymbol] ? "ON" : "OFF"}
                  </button>
                  <span className="coverage-recording-status">
                    {workloadControls.live_streaming_enabled && pairRecordingPrefs[activeSymbol]
                      ? `Recording: ${activeSymbol}`
                      : "Recording: Off"}
                  </span>
                </div>
              </>
            ) : null}
            <div className="small-title">Feed Delay (Per Tab)</div>
            <div className="coverage-cadence-grid">
              <label className="coverage-cadence-control">
                <span>On-screen delay</span>
                <input
                  type="range"
                  min={FEED_DELAY_MIN_MS}
                  max={FEED_DELAY_MAX_MS}
                  step={100}
                  value={activeDraftFeedCadence.on_screen_ms}
                  onChange={(event) => handleFeedCadenceChange("on_screen_ms", Number(event.target.value))}
                  disabled={!workloadControls.live_streaming_enabled}
                />
                <strong>{formatFeedDelayLabel(activeDraftFeedCadence.on_screen_ms)}</strong>
              </label>
              <label className="coverage-cadence-control">
                <span>Background delay</span>
                <input
                  type="range"
                  min={FEED_DELAY_MIN_MS}
                  max={FEED_DELAY_MAX_MS}
                  step={100}
                  value={activeDraftFeedCadence.background_ms}
                  onChange={(event) => handleFeedCadenceChange("background_ms", Number(event.target.value))}
                  disabled={!workloadControls.live_streaming_enabled}
                />
                <strong>{formatFeedDelayLabel(activeDraftFeedCadence.background_ms)}</strong>
              </label>
              <label className="coverage-cadence-control">
                <span>Render delay</span>
                <input
                  type="range"
                  min={FEED_DELAY_MIN_MS}
                  max={FEED_DELAY_MAX_MS}
                  step={100}
                  value={activeDraftFeedCadence.render_ms}
                  onChange={(event) => handleFeedCadenceChange("render_ms", Number(event.target.value))}
                  disabled={!workloadControls.live_streaming_enabled}
                />
                <strong>{formatFeedDelayLabel(activeDraftFeedCadence.render_ms)}</strong>
              </label>
            </div>
            <div className="coverage-bulk-actions">
              <button type="button" onClick={handleCoverageCheckAll} disabled={coverageApplying || !sourceCoverage}>
                Check All
              </button>
              <button type="button" onClick={handleCoverageUncheckAll} disabled={coverageApplying || !sourceCoverage}>
                Uncheck All
              </button>
            </div>
            <div className="coverage-list">
              <div className="coverage-list-head">
                <span>Exchange</span>
                <span>Status</span>
                <span>Heatmap/Liq</span>
                <span>Candles</span>
                <span>Trades</span>
                <span>Mark/Funding</span>
                <span>Liquidations</span>
              </div>
              {(sourceCoverage?.exchanges ?? []).map((item) => (
                <div key={item.exchange_id} className="coverage-row">
                  <label className="coverage-row-left">
                    <input
                      type="checkbox"
                      checked={coverageDraftEnabledIds ? coverageDraftEnabledIds.has(item.exchange_id) : item.enabled_by_user}
                      disabled={coverageApplying}
                      onChange={(event) => handleCoverageDraftToggle(item.exchange_id, event.target.checked)}
                    />
                    <span>{item.name}</span>
                  </label>
                  <span className="coverage-col-status">
                    {item.enabled_by_user
                      ? (item.connected ? "Live" : formatCoverageReason(item.reason))
                      : "Disabled"}
                  </span>
                  {(["book", "candles", "trades", "mark_funding", "liquidations"] as const).map((featureName) => {
                    const state = featureState(item, featureName);
                    return (
                      <span key={`${item.exchange_id}-${featureName}`} className={`coverage-feature-state ${state}`}>
                        {featureStateLabel(state)}
                      </span>
                    );
                  })}
                </div>
              ))}
            </div>
            <div className="coverage-actions">
              <div className={`coverage-draft-note ${coverageDraftDirty ? "dirty" : ""}`}>
                {coverageDraftDirty || coverageCadenceDirty ? "Unsaved changes" : "No pending changes"}
              </div>
              <div className="coverage-action-buttons">
                <button type="button" onClick={closeCoverageModal} disabled={coverageApplying}>
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => void handleApplyCoverageSelection()}
                  disabled={coverageApplying || !(coverageDraftDirty || coverageCadenceDirty)}
                >
                  {coverageApplying ? "Applying..." : "Apply"}
                </button>
              </div>
            </div>
            {coverageDraftError ? <div className="coverage-error">{coverageDraftError}</div> : null}
          </section>
        </div>
      ) : null}
    </div>
  );
}
