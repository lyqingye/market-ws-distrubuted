package com.tqxd.jys.core.message.kline;


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

  public KlineSnapshotMeta copy() {
    KlineSnapshotMeta meta = new KlineSnapshotMeta();
    meta.setPeriod(this.period);
    meta.setSymbol(this.symbol);
    meta.setCommittedIndex(this.committedIndex);
    meta.setTs(this.ts);
    return meta;
  }
}
