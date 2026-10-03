import { TerminalSnapshot } from "../../types/market";

export default function AbsorptionPanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  const value = snapshot ? Math.max(0, snapshot.trade_rate_10s - snapshot.spread) : 0;
  return (
    <section className="panel">
      <div className="small-title">Absorption</div>
      <div className="kv"><span>Proxy score</span><span>{value.toFixed(2)}</span></div>
      <div className="kv"><span>Spread</span><span>{snapshot?.spread?.toFixed(2) ?? "-"}</span></div>
    </section>
  );
}
