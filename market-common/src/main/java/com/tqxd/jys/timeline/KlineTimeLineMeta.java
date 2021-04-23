package com.tqxd.jys.timeline;

import com.tqxd.jys.utils.HuoBiUtils;

public class KlineTimeLineMeta {
  private String klineKey;
  private String detailKey;
  private long commitIndex;

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

  protected void applyCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }

  protected void incCommitIndex() {
    this.commitIndex++;
  }
}
