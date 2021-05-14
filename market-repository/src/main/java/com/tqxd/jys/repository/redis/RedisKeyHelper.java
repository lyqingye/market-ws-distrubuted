package com.tqxd.jys.repository.redis;

import com.tqxd.jys.constance.Period;

/**
 * redis key管理器
 * <p>
 * market:
 * -symbols
 * -BTC、ETH
 * -depth
 * -trade:
 * -detail
 * -kline:
 * -meta
 * -data
 *
 * @author lyqingye
 */
public class RedisKeyHelper {

  /**
   * 所有交易对的key
   */
  public static String getSymbolsKey() {
    return "market:symbols";
  }

  /**
   * k线元数据的key
   *
   * @param symbol 交易对
   */
  public static String toKlineMetaKey(String symbol, Period period) {
    return "market:" + symbol + ":kline:meta:" + period;
  }

  /**
   * k线数据的key
   *
   * @param symbol 交易对
   */
  public static String toKlineDataKey(String symbol, Period period) {
    return "market:" + symbol + ":kline:data:" + period;
  }

  /**
   * 市场详情的key
   *
   * @param symbol 交易对
   */
  public static String toMarketDetailKey(String symbol) {
    return "market:" + symbol + ":detail";
  }

  /**
   * 深度的key
   *
   * @param symbol 交易对
   */
  public static String toDepthKey(String symbol) {
    return "market:" + symbol + ":depth";
  }

  /**
   * 交易详情key
   *
   * @param symbol 交易对
   */
  public static String toTradeDetailKey(String symbol) {
    return "market:" + symbol + ":trade:detail";
  }
}
