package com.tqxd.jys.constance;

import java.util.concurrent.TimeUnit;

/**
 * @author ex
 */

public enum Period {
  /**
   *
   */
  _1_MIN("1min", TimeUnit.MINUTES.toMillis(1), 1440),
  _5_MIN("5min", TimeUnit.MINUTES.toMillis(5), 1440),
  _15_MIN("15min", TimeUnit.MINUTES.toMillis(15), 1440),
  _30_MIN("30min", TimeUnit.MINUTES.toMillis(30), 1440),
  _60_MIN("60min", TimeUnit.MINUTES.toMillis(60), 1440),
  _1_DAY("1day", TimeUnit.DAYS.toMillis(1), 360);
//    _1_WEEK("1week",TimeUnit.DAYS.toMillis(7), 258);

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

  public static Period ofName(String name) {
    for (Period value : values()) {
      if (value.equals(name)) {
        return value;
      }
    }
    return null;
  }
}
