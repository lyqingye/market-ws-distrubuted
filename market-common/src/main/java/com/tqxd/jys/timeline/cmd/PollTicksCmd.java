package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;

import java.util.List;

public class PollTicksCmd {
  private CmdResult<List<KlineTick>> result = new CmdResult<>();
  private long startTime, endTime;
  private int partIdx;

  public CmdResult<List<KlineTick>> getResult() {
    return result;
  }

  public void setResult(CmdResult<List<KlineTick>> result) {
    this.result = result;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public int getPartIdx() {
    return partIdx;
  }

  public void setPartIdx(int partIdx) {
    this.partIdx = partIdx;
  }
}
