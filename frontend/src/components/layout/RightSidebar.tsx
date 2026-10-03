import MarkFundingPanel from "../chart/MarkFundingPanel";
import SpreadPanel from "../chart/SpreadPanel";
import TapePanel from "../chart/TapePanel";
import { TerminalSnapshot } from "../../types/market";

export default function RightSidebar({ snapshot }: { snapshot: TerminalSnapshot | null }) {
  return (
    <>
      <SpreadPanel snapshot={snapshot} />
      <MarkFundingPanel snapshot={snapshot} />
      <TapePanel snapshot={snapshot} />
    </>
  );
}
