package com.tqxd.jys.core.kline.cmd;


import com.tqxd.jys.core.kline.KLineMeta;
import com.tqxd.jys.core.message.detail.MarketDetailTick;
import com.tqxd.jys.core.message.kline.KlineTick;

public class AppendTickResult {
  private KLineMeta meta;
  private KlineTick tick;
  private MarketDetailTick detail;

  public AppendTickResult(KLineMeta meta, KlineTick tick, MarketDetailTick detail) {
    this.meta = meta;
    this.tick = tick;
    this.detail = detail;
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

  public MarketDetailTick getDetail() {
    return detail;
  }

  public void setDetail(MarketDetailTick detail) {
    this.detail = detail;
  }
}
