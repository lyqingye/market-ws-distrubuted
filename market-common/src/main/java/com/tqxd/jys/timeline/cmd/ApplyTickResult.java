package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;

public class ApplyTickResult {
  private long committedIndex;
  private TemplatePayload<KlineTick> tick;
  private TemplatePayload<MarketDetailTick> detail;

  public ApplyTickResult(long committedIndex, TemplatePayload<KlineTick> tick, TemplatePayload<MarketDetailTick> detail) {
    this.committedIndex = committedIndex;
    this.tick = tick;
    this.detail = detail;
  }

  public TemplatePayload<KlineTick> getTick() {
    return tick;
  }

  public long getCommittedIndex() {
    return committedIndex;
  }

  public void setCommittedIndex(long committedIndex) {
    this.committedIndex = committedIndex;
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
