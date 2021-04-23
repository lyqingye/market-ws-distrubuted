package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.timeline.KlineTimeLineMeta;

public class UpdateTickResult {
  private KlineTick tick;
  private KlineTimeLineMeta meta;
  private MarketDetailTick detail;

  public UpdateTickResult(KlineTimeLineMeta meta, KlineTick tick, MarketDetailTick detail) {
    this.meta = meta;
    this.tick = tick;
    this.detail = detail;
  }

  public KlineTick getTick() {
    return tick;
  }

  public void setTick(KlineTick tick) {
    this.tick = tick;
  }

  public MarketDetailTick getDetail() {
    return detail;
  }

  public void setDetail(MarketDetailTick detail) {
    this.detail = detail;
  }

  public KlineTimeLineMeta getMeta() {
    return meta;
  }

  public void setMeta(KlineTimeLineMeta meta) {
    this.meta = meta;
  }
}
