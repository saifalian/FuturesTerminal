import { MarketEvent } from "../../types/market";
import { TerminalSnapshot } from "../../types/market";

export default function CandlesChart({ events, snapshot }: { events: MarketEvent[]; snapshot: TerminalSnapshot | null }) {
  const lastKline = [...events].reverse().find((e) => e.stream.includes("kline"));
  return (
    <section className="panel">
      <div className="small-title">Candles</div>
      <div className="kv"><span>Interval</span><span>{snapshot?.last_kline_interval ?? "1m"}</span></div>
      <div className="kv"><span>Close</span><span>{snapshot?.last_kline_close || "-"}</span></div>
      <div className="kv"><span>Last event</span><span>{lastKline ? lastKline.received_at : "n/a"}</span></div>
    </section>
  );
}
