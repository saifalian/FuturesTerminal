import { TerminalWsMessage } from "../types/market";

export class WsClient {
  private tabId: string;
  private symbol = "";
  private ws?: WebSocket;
  private reconnectTimer?: number;
  private manuallyClosed = false;
  private messageHandler?: (message: TerminalWsMessage) => void;
  private temporarilyBlockedUntilMs = 0;

  constructor(tabId: string = "default") {
    this.tabId = tabId;
  }

  setExpectedSymbol(symbol: string) {
    this.symbol = String(symbol || "").trim().toUpperCase();
  }

  connect(onMessage: (message: TerminalWsMessage) => void) {
    this.messageHandler = onMessage;
    this.manuallyClosed = false;
    if (Date.now() < this.temporarilyBlockedUntilMs) {
      return;
    }
    if (this.reconnectTimer) {
      window.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }
    const scheme = window.location.protocol === "https:" ? "wss" : "ws";
    const host = "127.0.0.1:8000";
    const query = new URLSearchParams({ tab_id: this.tabId });
    if (this.symbol) {
      query.set("symbol", this.symbol);
    }
    this.ws = new WebSocket(`${scheme}://${host}/ws/terminal?${query.toString()}`);
    this.ws.onmessage = (msg) => {
      try {
        const parsed = JSON.parse(msg.data) as TerminalWsMessage;
        if (
          parsed?.type === "market_event" ||
          parsed?.type === "terminal_snapshot" ||
          parsed?.type === "symbol_changed" ||
          parsed?.type === "heatmap_frame" ||
          parsed?.type === "ladder_snapshot" ||
          parsed?.type === "source_coverage_update" ||
          parsed?.type === "symbol_resolution" ||
          parsed?.type === "ml_run_update" ||
          parsed?.type === "ml_run_log" ||
          parsed?.type === "ml_bot_update" ||
          parsed?.type === "ml_training_epoch" ||
          parsed?.type === "ml_training_batch"
        ) {
          this.messageHandler?.(parsed);
        }
      } catch {
        // ignore malformed payloads
      }
    };
    this.ws.onopen = () => {
      this.ws?.send("ping");
    };
    this.ws.onclose = (event) => {
      this.ws = undefined;
      if (this.manuallyClosed) return;
      const reason = String(event.reason || "").toLowerCase();
      // When live streaming is disabled backend closes with 1013 + reason.
      // Pause reconnect attempts to avoid busy reconnect loops/spam.
      if (event.code === 1013 || reason.includes("live streaming disabled")) {
        this.temporarilyBlockedUntilMs = Date.now() + 15_000;
        return;
      }
      this.reconnectTimer = window.setTimeout(() => {
        if (this.messageHandler) {
          this.connect(this.messageHandler);
        }
      }, 1000);
    };
  }

  close() {
    this.manuallyClosed = true;
    this.temporarilyBlockedUntilMs = 0;
    if (this.reconnectTimer) {
      window.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.ws?.close();
    this.ws = undefined;
  }
}
