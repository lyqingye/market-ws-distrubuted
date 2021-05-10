package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.timeline.KLineMeta;

public class AppendTickResult {
  private KLineMeta meta;
  private KlineTick tick;
  private KlineTick detail;

  public AppendTickResult(KLineMeta meta, KlineTick tick, KlineTick detail) {
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

  public KlineTick getDetail() {
    return detail;
  }

  public void setDetail(KlineTick detail) {
    this.detail = detail;
  }
}
