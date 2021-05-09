package com.tqxd.jys.core.kline.cmd;

import com.tqxd.jys.core.kline.KLineMeta;
import com.tqxd.jys.core.message.detail.MarketDetailTick;

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
