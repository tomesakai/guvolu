import type { ReactElement } from "react";
import type { L2QualityResponse } from "./api";
import { Badge } from "./panels";
import { l2QualityView } from "./l2-quality-view";

export function L2QualityStrip({
  quality,
}: {
  quality: L2QualityResponse | null;
}): ReactElement {
  const view = l2QualityView(quality);
  const threshold = quality?.freshness_threshold_seconds ?? 720;
  const hint =
    `仅物化新鲜度，阈值 ${String(threshold)} 秒；` +
    "不表示 wire 或 checkpoint 新鲜度";
  return (
    <div className="l2-quality-strip" title={hint} aria-label="L2 物化质量">
      <Badge tone={view.tone} text={`L2 ${view.status}`} hint={hint} />
      <span>{`物化 ${view.freshness}`}</span>
      <span>{`checksum ${view.checksum}`}</span>
      <span>{`anchor hard ${view.anchorHard}`}</span>
      <span>{`seq hard ${view.sequenceHard}`}</span>
      {view.clockSkewed ? <span className="l2-quality-strip__skew">clock_skewed</span> : null}
    </div>
  );
}
