package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;

public class ApplyTickResult {
  private TemplatePayload<KlineTick> tick;
  private TemplatePayload<MarketDetailTick> detail;

  public ApplyTickResult(TemplatePayload<KlineTick> tick, TemplatePayload<MarketDetailTick> detail) {
    this.tick = tick;
    this.detail = detail;
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
