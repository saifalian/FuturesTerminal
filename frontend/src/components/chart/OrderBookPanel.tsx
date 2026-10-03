import { TerminalSnapshot } from "../../types/market";

export default function OrderBookPanel({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  const streamCounts = snapshot?.stream_counts ?? {};
  const depthCount = Object.entries(streamCounts)
    .filter(([stream]) => stream.includes("@depth"))
    .reduce((acc, [, value]) => acc + Number(value || 0), 0);
  return (
    <section className="panel">
      <div className="small-title">DOM / Order Book</div>
      <div className="kv"><span>Best bid</span><span className="buy">{snapshot?.best_bid || "-"}</span></div>
      <div className="kv"><span>Best ask</span><span className="sell">{snapshot?.best_ask || "-"}</span></div>
      <div className="kv"><span>Spread</span><span>{snapshot?.spread?.toFixed(2) ?? "-"}</span></div>
      <div className="kv"><span>Depth updates</span><span>{depthCount}</span></div>
      <div className="kv"><span>Levels (bids/asks)</span><span>{snapshot ? `${snapshot.depth_levels.bids}/${snapshot.depth_levels.asks}` : "-"}</span></div>
    </section>
  );
}
