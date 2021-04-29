package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.timeline.KLineMeta;

public class ApplyTickResult {
  private KLineMeta meta;
  private TemplatePayload<KlineTick> tick;
  private TemplatePayload<MarketDetailTick> detail;

  public ApplyTickResult(KLineMeta meta, TemplatePayload<KlineTick> tick, TemplatePayload<MarketDetailTick> detail) {
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

  public TemplatePayload<KlineTick> getTick() {
    return tick;
  }

  public void setTick(TemplatePayload<KlineTick> tick) {
    this.tick = tick;
  }

  public TemplatePayload<MarketDetailTick> getDetail() {
    return detail;
  }

  public void setDetail(TemplatePayload<MarketDetailTick> detail) {
    this.detail = detail;
  }
}
