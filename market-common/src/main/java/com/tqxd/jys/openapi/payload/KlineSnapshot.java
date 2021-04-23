package com.tqxd.jys.openapi.payload;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;

import java.util.Collections;
import java.util.List;

/**
 * kline 快照
 */
public class KlineSnapshot {

  /**
   * 交易对
   */
  private String klineKey;
  /**
   * 时间间隔
   */
  private Period period;
  /**
   * 快照最后日志索引
   */
  private long committedIndex;
  /**
   * k线tick
   */
  private List<KlineTick> tickList;

  public KlineSnapshot() {
  }


  public String getKlineKey() {
    return klineKey;
  }

  public void setKlineKey(String klineKey) {
    this.klineKey = klineKey;
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

  public List<KlineTick> getTickList() {
    return tickList == null ? Collections.emptyList() : tickList;
  }

  public void setTickList(List<KlineTick> tickJsonStrList) {
    this.tickList = tickJsonStrList;
  }
}
