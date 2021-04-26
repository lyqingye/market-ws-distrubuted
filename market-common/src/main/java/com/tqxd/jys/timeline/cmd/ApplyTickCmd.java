package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;

public class ApplyTickCmd {
  private CmdResult<ApplyTickResult> result = new CmdResult<>();
  private long commitIndex;
  private KlineTick tick;

  public CmdResult<ApplyTickResult> getResult() {
    return result;
  }

  public void setResult(CmdResult<ApplyTickResult> result) {
    this.result = result;
  }

  public KlineTick getTick() {
    return tick;
  }

  public void setTick(KlineTick tick) {
    this.tick = tick;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }
}
