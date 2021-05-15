package com.tqxd.jys.constance;

import java.util.concurrent.TimeUnit;

/**
 * @author ex
 */

public enum Period {
  /**
   * k线时间档
   */
  _1_MIN("1min", TimeUnit.MINUTES.toSeconds(1), 1440),
  _5_MIN("5min", TimeUnit.MINUTES.toSeconds(5), 1440),
  _15_MIN("15min", TimeUnit.MINUTES.toSeconds(15), 1440),
  _30_MIN("30min", TimeUnit.MINUTES.toSeconds(30), 1440),
  _60_MIN("60min", TimeUnit.MINUTES.toSeconds(60), 1440),
  _4_HOUR("4hour", TimeUnit.MINUTES.toSeconds(30), 1440),
  _1_DAY("1day", TimeUnit.DAYS.toSeconds(1), 360),
  _1_WEEK("1week", TimeUnit.DAYS.toSeconds(7), 258);

  private final String symbol;
  private final long mill;
  private final int numOfPeriod;

  Period(String symbol, long mill, int numOfPeriod) {
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

  public static Period valueOfSymbol(String symbol) {
    for (Period value : values()) {
      if (value.symbol.equalsIgnoreCase(symbol)) {
        return value;
      }
    }
    return null;
  }

  public static Period valueOfName(String name) {
    for (Period value : values()) {
      if (value.name().equalsIgnoreCase(name)) {
        return value;
      }
    }
    return null;
  }

  public static Period containsSymbol(String symbol) {
    for (Period value : values()) {
      if (value.symbol.contains(symbol)) {
        return value;
      }
    }
    return null;
  }


}
