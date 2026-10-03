export type WorkspacePresetValues = {
  timeframe: string;
  showHeatmap: boolean;
  thresholdLow: number;
  thresholdHigh: number;
  rowSizeMultiplier: number;
  heatmapCoverageScale: number;
};

export type WorkspacePresetStore = {
  activePreset: string;
  presets: Record<string, WorkspacePresetValues>;
};

const STORAGE_KEY = "futures_terminal_workspace_presets_v1";

const DEFAULT_PRESET_NAME = "Default";
const DEFAULT_PRESET: WorkspacePresetValues = {
  timeframe: "1m",
  showHeatmap: true,
  thresholdLow: 0.06,
  thresholdHigh: 0.88,
  rowSizeMultiplier: 1,
  heatmapCoverageScale: 1,
};

function isBrowser() {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function sanitizePreset(raw: Partial<WorkspacePresetValues> | null | undefined): WorkspacePresetValues {
  const value = raw ?? {};
  const timeframe = typeof value.timeframe === "string" && value.timeframe.trim() ? value.timeframe : DEFAULT_PRESET.timeframe;
  const showHeatmap = typeof value.showHeatmap === "boolean" ? value.showHeatmap : DEFAULT_PRESET.showHeatmap;
  const thresholdLow = clamp(Number(value.thresholdLow ?? DEFAULT_PRESET.thresholdLow), 0, 1);
  const thresholdHigh = clamp(Number(value.thresholdHigh ?? DEFAULT_PRESET.thresholdHigh), 0, 3);
  const rowSizeMultiplier = clamp(Math.round(Number(value.rowSizeMultiplier ?? DEFAULT_PRESET.rowSizeMultiplier)), 1, 50);
  const heatmapCoverageScale = clamp(Number(value.heatmapCoverageScale ?? DEFAULT_PRESET.heatmapCoverageScale), 1, 500);
  return {
    timeframe,
    showHeatmap,
    thresholdLow,
    thresholdHigh,
    rowSizeMultiplier,
    heatmapCoverageScale,
  };
}

function defaultStore(): WorkspacePresetStore {
  return {
    activePreset: DEFAULT_PRESET_NAME,
    presets: {
      [DEFAULT_PRESET_NAME]: { ...DEFAULT_PRESET },
    },
  };
}

function sanitizeStore(raw: unknown): WorkspacePresetStore {
  if (!raw || typeof raw !== "object") return defaultStore();
  const input = raw as Partial<WorkspacePresetStore>;
  const presetsRaw = input.presets && typeof input.presets === "object" ? input.presets : {};
  const presets: Record<string, WorkspacePresetValues> = {};
  for (const [name, value] of Object.entries(presetsRaw)) {
    const cleanName = String(name || "").trim();
    if (!cleanName) continue;
    presets[cleanName] = sanitizePreset(value as Partial<WorkspacePresetValues>);
  }
  if (!Object.keys(presets).length) {
    presets[DEFAULT_PRESET_NAME] = { ...DEFAULT_PRESET };
  }
  const active = String(input.activePreset || "").trim();
  const activePreset = presets[active] ? active : Object.keys(presets)[0];
  return { activePreset, presets };
}

function emitPresetUpdated(store: WorkspacePresetStore) {
  if (!isBrowser()) return;
  window.dispatchEvent(new CustomEvent("workspace-preset-updated", { detail: store }));
}

export function loadWorkspacePresetStore(): WorkspacePresetStore {
  if (!isBrowser()) return defaultStore();
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return defaultStore();
    return sanitizeStore(JSON.parse(raw));
  } catch {
    return defaultStore();
  }
}

export function saveWorkspacePresetStore(store: WorkspacePresetStore): WorkspacePresetStore {
  const safeStore = sanitizeStore(store);
  if (isBrowser()) {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(safeStore));
    emitPresetUpdated(safeStore);
  }
  return safeStore;
}

export function getActiveWorkspacePresetValues(): WorkspacePresetValues {
  const store = loadWorkspacePresetStore();
  return sanitizePreset(store.presets[store.activePreset]);
}

export function updateActivePresetValues(patch: Partial<WorkspacePresetValues>): WorkspacePresetStore {
  const store = loadWorkspacePresetStore();
  const name = store.activePreset;
  const current = sanitizePreset(store.presets[name]);
  const merged = sanitizePreset({ ...current, ...patch });
  return saveWorkspacePresetStore({
    ...store,
    presets: {
      ...store.presets,
      [name]: merged,
    },
  });
}

export function activatePreset(name: string): WorkspacePresetStore {
  const store = loadWorkspacePresetStore();
  if (!store.presets[name]) return store;
  return saveWorkspacePresetStore({
    ...store,
    activePreset: name,
  });
}

export function upsertPreset(name: string, values: WorkspacePresetValues): WorkspacePresetStore {
  const cleanName = String(name || "").trim();
  if (!cleanName) return loadWorkspacePresetStore();
  const store = loadWorkspacePresetStore();
  return saveWorkspacePresetStore({
    activePreset: cleanName,
    presets: {
      ...store.presets,
      [cleanName]: sanitizePreset(values),
    },
  });
}

export function removePreset(name: string): WorkspacePresetStore {
  const store = loadWorkspacePresetStore();
  if (!store.presets[name]) return store;
  const nextPresets = { ...store.presets };
  delete nextPresets[name];
  if (!Object.keys(nextPresets).length) {
    nextPresets[DEFAULT_PRESET_NAME] = { ...DEFAULT_PRESET };
  }
  const nextActive = nextPresets[store.activePreset] ? store.activePreset : Object.keys(nextPresets)[0];
  return saveWorkspacePresetStore({
    activePreset: nextActive,
    presets: nextPresets,
  });
}
