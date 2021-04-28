package com.tqxd.jys.timeline;

import com.tqxd.jys.constance.Period;

public class KLineMeta {
  private String symbol;
  private Period period;
  private long commitIndex;

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public Period getPeriod() {
    return period;
  }

  public void setPeriod(Period period) {
    this.period = period;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  protected void applyCommittedIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }

  protected void incCommitIndex() {
    this.commitIndex++;
  }
}
