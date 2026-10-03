import { TerminalSnapshot } from "../../types/market";

export default function TapePanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  return (
    <section className="panel right-sidebar-panel right-sidebar-panel--tape">
      <div className="small-title">Tape (AggTrades)</div>
      <div className="kv"><span>Trade speed (10s)</span><span>{snapshot?.trade_rate_10s.toFixed(2) ?? "0.00"}/s</span></div>
    </section>
  );
}
