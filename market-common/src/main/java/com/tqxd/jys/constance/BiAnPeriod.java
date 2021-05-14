package com.tqxd.jys.constance;

import java.util.concurrent.TimeUnit;

/**
 * @author ex
 */

public enum BiAnPeriod{
  /**
   * k线时间档
   */
  _1_MIN("1m", TimeUnit.MINUTES.toSeconds(1), 1440),
  _5_MIN("5m", TimeUnit.MINUTES.toSeconds(5), 1440),
  _15_MIN("15m", TimeUnit.MINUTES.toSeconds(15), 1440),
  _30_MIN("30m", TimeUnit.MINUTES.toSeconds(30), 1440),
  _60_MIN("60m", TimeUnit.MINUTES.toSeconds(60), 1440),
  _4_HOUR("4h", TimeUnit.MINUTES.toSeconds(30), 1440),
  _1_DAY("1d", TimeUnit.DAYS.toSeconds(1), 360),
  _1_WEEK("1w", TimeUnit.DAYS.toSeconds(7), 258);

  private final String symbol;
  private final long mill;
  private final int numOfPeriod;

  BiAnPeriod(String symbol, long mill, int numOfPeriod) {
    this.symbol = symbol;
    this.mill = mill;
    this.numOfPeriod = numOfPeriod;
  }

  public String getSymbol() {
    return this.symbol;
  }

  public long getMill() {
    return this.mill;
  }

  public int getNumOfPeriod() {
    return this.numOfPeriod;
  }

  public static BiAnPeriod valueOfSymbol(String symbol) {
    for (BiAnPeriod value : values()) {
      if (value.symbol.equalsIgnoreCase(symbol)) {
        return value;
      }
    }
    return null;
  }


  public static Period containsSymbol(String symbol) {
    for (Period value : Period.values()) {
      if (value.getSymbol().contains(symbol)) {
        return value;
      }
    }
    return null;
  }


}
