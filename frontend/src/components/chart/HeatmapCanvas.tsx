import { useMemo } from "react";
import { MarketEvent } from "../../types/market";
import { TerminalSnapshot } from "../../types/market";

export default function HeatmapCanvas({ events, snapshot }: { events: MarketEvent[]; snapshot: TerminalSnapshot | null }) {
  const depthCount = useMemo(() => events.filter((e) => e.stream.includes("depth")).length, [events]);
  return (
    <section className="panel">
      <div className="small-title">Liquidity Heatmap</div>
      <div className="kv"><span>Depth updates</span><span>{depthCount}</span></div>
      <div className="kv"><span>Book synced</span><span>{snapshot?.book_synced ? "YES" : "NO"}</span></div>
      <div className="kv"><span>Levels</span><span>{snapshot ? `${snapshot.depth_levels.bids}/${snapshot.depth_levels.asks}` : "-"}</span></div>
      <div style={{ marginTop: 8, height: 180, background: "linear-gradient(180deg, #3b1f1f, #1b3c38)" }} />
    </section>
  );
}
