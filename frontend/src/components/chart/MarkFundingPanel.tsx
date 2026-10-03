import { TerminalSnapshot } from "../../types/market";

export default function MarkFundingPanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  return (
    <section className="panel right-sidebar-panel right-sidebar-panel--mark">
      <div className="small-title">Mark / Funding</div>
      <div className="kv"><span>Mark</span><span>{snapshot?.mark_price ?? "-"}</span></div>
      <div className="kv"><span>Funding</span><span>{snapshot?.funding_rate ?? "-"}</span></div>
    </section>
  );
}
