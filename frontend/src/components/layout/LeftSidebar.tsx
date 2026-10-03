import OrderBookPanel from "../chart/OrderBookPanel";
import OrderEntry from "../trading/OrderEntry";
import PositionPanel from "../trading/PositionPanel";
import OpenOrdersPanel from "../trading/OpenOrdersPanel";
import { TerminalSnapshot } from "../../types/market";

type LeftSidebarProps = {
  snapshot: TerminalSnapshot | null;
  hideTradingPanels?: boolean;
};

export default function LeftSidebar({ snapshot, hideTradingPanels = false }: LeftSidebarProps) {
  return (
    <>
      <OrderBookPanel snapshot={snapshot} />
      {!hideTradingPanels ? (
        <>
          <OrderEntry />
          <PositionPanel />
          <OpenOrdersPanel />
        </>
      ) : null}
    </>
  );
}
