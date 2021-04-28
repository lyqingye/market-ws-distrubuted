package com.tqxd.jys.utils;

import com.tqxd.jys.common.payload.TemplatePayload;

/**
 * 用于处理 {@link TemplatePayload#getCh()} 的数据
 */
public class ChannelUtil {
  /**
   * 1. k线channel
   * market.$symbol.kline
   * <p>
   * 2. 深度channel
   * market.$symbol.depth
   * <p>
   * 3. 交易详情channel
   * market.$symbol.trade.detail
   * <p>
   * 4. 市场详情channel
   * market.$symbol.detail
   */
  private static final String DOT = "\\.";

  /**
   * 获取交易对
   *
   * @param ch channel
   * @return 交易对 or null
   */
  public static String getSymbol(String ch) {
    if (ch == null || ch.isEmpty()) {
      return null;
    }
    String[] split = ch.split(DOT);
    if (split.length >= 2) {
      return split[1];
    }
    return null;
  }

  /**
   * 是否为k线channel
   *
   * @param ch channel
   */
  public static boolean isKLineChannel(String ch) {
    if (ch == null || ch.isEmpty()) {
      return false;
    }
    String[] split = ch.split(DOT);
    if (split.length >= 2) {
      return "kline".equalsIgnoreCase(split[2]);
    }
    return false;
  }

  /**
   * 是否为深度channel
   *
   * @param ch channel
   */
  public static boolean isDepthChannel(String ch) {
    if (ch == null || ch.isEmpty()) {
      return false;
    }
    String[] split = ch.split(DOT);
    if (split.length >= 3) {
      return "depth".equalsIgnoreCase(split[2]);
    }
    return false;
  }

  /**
   * 是否为交易详情channel
   *
   * @param ch channel
   */
  public static boolean isTradeDetailChannel(String ch) {
    if (ch == null || ch.isEmpty()) {
      return false;
    }
    String[] split = ch.split(DOT);
    if (split.length >= 4) {
      return "depth".equalsIgnoreCase(split[3]);
    }
    return false;
  }

  /**
   * 是否为市场概括channel
   *
   * @param ch channel
   */
  public static boolean isMarketDetailChannel(String ch) {
    if (ch == null || ch.isEmpty()) {
      return false;
    }
    String[] split = ch.split(DOT);
    if (split.length >= 3) {
      return "detail".equalsIgnoreCase(split[2]);
    }
    return false;
  }
}
