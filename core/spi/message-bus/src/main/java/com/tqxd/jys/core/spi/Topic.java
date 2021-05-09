package com.tqxd.jys.core.spi;

public enum Topic {
  /**
   * K线tick数据主题 (多笔交易合成)
   */
  KLINE_TICK_TOPIC,

  /**
   * 本交易细节数据主题 (单笔交易)
   */
  TRADE_DETAIL_TOPIC,

  /**
   * 深度数据主题
   */
  DEPTH_CHART_TOPIC,

  /**
   * 最新市场价格主题
   */
  MARKET_PRICE_TOPIC,
}

