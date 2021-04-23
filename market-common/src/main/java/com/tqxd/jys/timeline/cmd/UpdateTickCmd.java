package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;

public class UpdateTickCmd {
  private CmdResult<UpdateTickResult> result = new CmdResult<>();
  private long commitIndex;
  private KlineTick tick;

  public CmdResult<UpdateTickResult> getResult() {
    return result;
  }

  public void setResult(CmdResult<UpdateTickResult> result) {
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
