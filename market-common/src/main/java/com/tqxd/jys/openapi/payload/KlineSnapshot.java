package com.tqxd.jys.openapi.payload;

import com.tqxd.jys.common.payload.KlineTick;

import java.util.Collections;
import java.util.List;

/**
 * kline 快照
 */
public class KlineSnapshot {

  /**
   * 元数据
   */
  private KlineSnapshotMeta meta;

  /**
   * k线tick
   */
  private List<KlineTick> tickList;

  public KlineSnapshot() {
  }

  public KlineSnapshotMeta getMeta() {
    return meta;
  }

  public void setMeta(KlineSnapshotMeta meta) {
    this.meta = meta;
  }

  public List<KlineTick> getTickList() {
    return tickList == null ? Collections.emptyList() : tickList;
  }

  public void setTickList(List<KlineTick> tickJsonStrList) {
    this.tickList = tickJsonStrList;
  }

  public KlineSnapshot copy () {
    KlineSnapshot snapshot = new KlineSnapshot();
    snapshot.meta = this.meta;
    snapshot.tickList = this.tickList;
    return snapshot;
  }

}
