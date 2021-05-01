package com.tqxd.jys.timeline.cmd;

import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;

/**
 * k线聚合结果
 *
 * @author lyqingye
 */
public class KLineAggregateResult {
  private String symbol;
  private MarketDetailTick tick;

  public KLineAggregateResult(String symbol, MarketDetailTick tick) {
    this.symbol = symbol;
    this.tick = tick;
  }

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public MarketDetailTick getTick() {
    return tick;
  }

  public void setTick(MarketDetailTick tick) {
    this.tick = tick;
  }
}
