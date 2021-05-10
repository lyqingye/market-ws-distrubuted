package com.tqxd.jys.timeline.cmd;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.timeline.KLineMeta;

/**
 * k线聚合结果
 *
 * @author lyqingye
 */
public class Auto24HourStatisticsResult {
  private KLineMeta meta;
  private KlineTick tick;

  public Auto24HourStatisticsResult(KLineMeta meta, KlineTick tick) {
    this.meta = meta;
    this.tick = tick;
  }

  public KLineMeta getMeta() {
    return meta;
  }

  public void setMeta(KLineMeta meta) {
    this.meta = meta;
  }

  public KlineTick getTick() {
    return tick;
  }

  public void setTick(KlineTick tick) {
    this.tick = tick;
  }
}
