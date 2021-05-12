package com.tqxd.jys.constance;

public enum DataType {
  /**
   * K线数据
   */
  KLINE,
  /**
   * 深度数据
   */
  DEPTH,
  /**
   * 成交数据
   */
  TRADE_DETAIL,

  /**
   * k线历史
   */
  KLINE_HISTORY;

  public static DataType valueOfName(String name) {
    for (DataType type : values()) {
      if (type.name().equalsIgnoreCase(name)) {
        return type;
      }
    }
    return null;
  }
}
