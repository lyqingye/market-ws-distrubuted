package com.tqxd.jys.timeline.cmd;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;

import java.util.Collections;
import java.util.List;

public class ApplySnapshotCmd {
  private CmdResult<MarketDetailTick> result = new CmdResult<>();
  private long commitIndex;
  private List<KlineTick> ticks = Collections.emptyList();

  public long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public List<KlineTick> getTicks() {
    return ticks;
  }

  public void setTicks(List<KlineTick> ticks) {
    this.ticks = ticks;
  }

  public CmdResult<MarketDetailTick> getResult() {
    return result;
  }

  public void setResult(CmdResult<MarketDetailTick> result) {
    this.result = result;
  }
}
