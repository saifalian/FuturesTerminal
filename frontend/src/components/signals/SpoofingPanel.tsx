import { TerminalSnapshot } from "../../types/market";

export default function SpoofingPanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  return (
    <section className="panel">
      <div className="small-title">Spoofing</div>
      <div className="kv"><span>Sync anomaly count</span><span>{snapshot?.sync_failures ?? 0}</span></div>
      <div className="kv"><span>Depth health</span><span>{snapshot?.book_synced ? "Stable" : "Resyncing"}</span></div>
    </section>
  );
}
