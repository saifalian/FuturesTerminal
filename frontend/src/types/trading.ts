export type Position = {
  symbol: string;
  side: "LONG" | "SHORT";
  qty: number;
  entry: number;
  pnl: number;
};

export type OpenOrder = {
  id: string;
  side: "BUY" | "SELL";
  price: number;
  qty: number;
};
