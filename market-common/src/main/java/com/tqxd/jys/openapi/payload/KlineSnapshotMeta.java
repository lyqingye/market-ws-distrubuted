package com.tqxd.jys.openapi.payload;

import com.tqxd.jys.constance.Period;

public class KlineSnapshotMeta {
  /**
   * 交易对
   */
  private String symbol;
  /**
   * 时间间隔
   */
  private Period period;
  /**
   * 快照最后日志索引
   */
  private long committedIndex;

  private long ts;

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

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

  public long getCommittedIndex() {
    return committedIndex;
  }

  public void setCommittedIndex(long committedIndex) {
    this.committedIndex = committedIndex;
  }
}
