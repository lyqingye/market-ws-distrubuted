package com.tqxd.jys.constance;

import java.util.concurrent.TimeUnit;

/**
 * @author ex
 */

public enum TqxdPeriod {
  /**
   * k线时间档
   */
  _1_MIN( 60, TimeUnit.MINUTES.toSeconds(1), 1440),
  _5_MIN( 5*60, TimeUnit.MINUTES.toSeconds(5), 1440),
  _15_MIN(15*60, TimeUnit.MINUTES.toSeconds(15), 1440),
  _30_MIN(30*60, TimeUnit.MINUTES.toSeconds(30), 1440),
  _60_MIN(60*60, TimeUnit.MINUTES.toSeconds(60), 1440),
  _4_HOUR(240*60, TimeUnit.MINUTES.toSeconds(30), 1440),
  _1_DAY( 1440*60, TimeUnit.DAYS.toSeconds(1), 360),
  _1_WEEK(1440*7*60, TimeUnit.DAYS.toSeconds(7), 258);

  private final int symbol;
  private final long mill;
  private final int numOfPeriod;

  TqxdPeriod(int symbol, long mill, int numOfPeriod) {
    this.symbol = symbol;
    this.mill = mill;
    this.numOfPeriod = numOfPeriod;
  }

  public int getSymbol() {
    return this.symbol;
  }

  public long getMill() {
    return this.mill;
  }

  public int getNumOfPeriod() {
    return this.numOfPeriod;
  }

  public static TqxdPeriod valueOfSymbol(int symbol) {
    for (TqxdPeriod value : values()) {
      if (value.symbol == symbol) {
        return value;
      }
    }
    return null;
  }



}
