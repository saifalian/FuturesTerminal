import { TerminalSnapshot } from "../../types/market";

export default function CvdPanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  const pressure = snapshot?.last_trade_side === "BUY" ? "Buy pressure" : snapshot?.last_trade_side === "SELL" ? "Sell pressure" : "Neutral";
  return (
    <section className="panel">
      <div className="small-title">CVD</div>
      <div className="kv"><span>Proxy state</span><span>{pressure}</span></div>
      <div className="kv"><span>Trade speed</span><span>{snapshot?.trade_rate_10s.toFixed(2) ?? "0.00"}/s</span></div>
    </section>
  );
}
