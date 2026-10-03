import { BiasSnapshot } from "../../types/signals";

export default function BiasMeter({ bias }: { bias: BiasSnapshot }) {
  const longPct = Math.round(bias.long_score * 10);
  const shortPct = Math.round(bias.short_score * 10);
  return (
    <section className="panel">
      <div className="small-title">Bias Meter</div>
      <div className="kv"><span>Long</span><span className="buy">{longPct}%</span></div>
      <div className="kv"><span>Short</span><span className="sell">{shortPct}%</span></div>
      <div className="kv"><span>Confidence</span><span>{Math.round(bias.confidence * 100)}%</span></div>
    </section>
  );
}
