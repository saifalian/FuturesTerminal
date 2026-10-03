import { TerminalSnapshot } from "../../types/market";

export default function PositioningPanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  const totalStreams = snapshot ? Object.keys(snapshot.stream_counts).length : 0;
  return (
    <section className="panel">
      <div className="small-title">Positioning Context</div>
      <div className="kv"><span>Streams active</span><span>{totalStreams}</span></div>
      <div className="kv"><span>Liquidations 60s</span><span>{snapshot?.liq_events_60s ?? 0}</span></div>
    </section>
  );
}
