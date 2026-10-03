import { TerminalSnapshot } from "../../types/market";

export default function SpreadPanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  const bid = snapshot?.best_bid ?? 0;
  const ask = snapshot?.best_ask ?? 0;
  const spread = bid > 0 && ask > 0 ? ask - bid : 0;
  return (
    <section className="panel right-sidebar-panel right-sidebar-panel--spread">
      <div className="small-title">Spread</div>
      <div className="kv"><span>Bid</span><span className="buy">{bid || "-"}</span></div>
      <div className="kv"><span>Ask</span><span className="sell">{ask || "-"}</span></div>
      <div className="kv"><span>Spread</span><span>{spread.toFixed(2)}</span></div>
    </section>
  );
}
