package com.tqxd.jys.servicebus.payload;

import com.tqxd.jys.constance.Period;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * kline 快照
 */
@DataObject
public class KlineSnapshotDto {

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
  /**
   * 快照时间戳
   */
  private long ts;
  /**
   * k线tick
   */
  private List<String> tickJsonStrList;

  public KlineSnapshotDto() {
  }

  public KlineSnapshotDto(JsonObject json) {
    KlineSnapshotDto t = json.mapTo(KlineSnapshotDto.class);
    this.symbol = t.symbol;
    this.committedIndex = t.committedIndex;
    this.period = t.period;
    this.tickJsonStrList = t.tickJsonStrList;
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

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

  public List<String> getTickJsonStrList() {
    return tickJsonStrList;
  }

  public void setTickJsonStrList(List<String> tickJsonStrList) {
    this.tickJsonStrList = tickJsonStrList;
  }
}
