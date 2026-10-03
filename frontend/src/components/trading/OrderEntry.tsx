import { useState } from "react";

export default function OrderEntry() {
  const [qty, setQty] = useState("0.001");
  return (
    <section className="panel">
      <div className="small-title">Order Entry</div>
      <div className="kv"><span>Qty</span><input value={qty} onChange={(e) => setQty(e.target.value)} /></div>
      <button style={{ marginRight: 8 }}>Buy Market</button>
      <button>Sell Market</button>
    </section>
  );
}
