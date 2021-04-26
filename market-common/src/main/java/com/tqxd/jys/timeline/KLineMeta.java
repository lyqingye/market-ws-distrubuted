package com.tqxd.jys.timeline;

import com.tqxd.jys.constance.Period;
import com.tqxd.jys.utils.HuoBiUtils;

public class KLineMeta {
  private String symbol;
  private Period period;
  private String klineKey;
  private String detailKey;
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

  public String getKlineKey() {
    return klineKey;
  }

  public void setKlineKey(String klineKey) {
    this.klineKey = klineKey;
    this.detailKey = HuoBiUtils.toDetailSub(HuoBiUtils.getSymbolFromKlineSub(klineKey));
  }

  public String getDetailKey() {
    return detailKey;
  }

  public void setDetailKey(String detailKey) {
    this.detailKey = detailKey;
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
