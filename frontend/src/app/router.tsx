import LogsPage from "../pages/LogsPage";
import ReplayPage from "../pages/ReplayPage";
import SettingsPage from "../pages/SettingsPage";
import TerminalPage from "../pages/TerminalPage";

export type RouteKey = "terminal" | "replay" | "settings" | "logs";

export function buildRoutes() {
  return {
    terminal: TerminalPage,
    replay: ReplayPage,
    settings: SettingsPage,
    logs: LogsPage,
  };
}
