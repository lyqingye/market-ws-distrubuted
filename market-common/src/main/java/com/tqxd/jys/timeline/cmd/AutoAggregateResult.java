package com.tqxd.jys.timeline.cmd;

import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.timeline.KLineMeta;

/**
 * k线聚合结果
 *
 * @author lyqingye
 */
public class AutoAggregateResult {
  private KLineMeta meta;
  private MarketDetailTick tick;

  public AutoAggregateResult(KLineMeta meta, MarketDetailTick tick) {
    this.meta = meta;
    this.tick = tick;
  }

  public KLineMeta getMeta() {
    return meta;
  }

  public void setMeta(KLineMeta meta) {
    this.meta = meta;
  }

  public MarketDetailTick getTick() {
    return tick;
  }

  public void setTick(MarketDetailTick tick) {
    this.tick = tick;
  }
}
