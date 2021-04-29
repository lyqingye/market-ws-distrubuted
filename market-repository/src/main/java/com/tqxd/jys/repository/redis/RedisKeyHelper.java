package com.tqxd.jys.repository.redis;

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
  public static String toKlineMetaKey(String symbol) {
    return "market:" + symbol + ":kline:meta";
  }

  /**
   * k线数据的key
   *
   * @param symbol 交易对
   */
  public static String toKlineDataKey(String symbol) {
    return "market:" + symbol + ":kline:data";
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
