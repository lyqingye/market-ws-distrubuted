package com.tqxd.jys.utils;

import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;

import java.util.HashMap;
import java.util.Map;

/**
 * 用于处理 {@link TemplatePayload#getCh()} 的数据
 */
public class ChannelUtil {
  /**
   * k线主题解析缓存
   */
  private static Map<String, KLineChannel> KLINE_CHANNEL_RESOLVE_CACHE = new HashMap<>(64);

  /**
   * 1. k线channel
   * market.$symbol.kline.$period
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
   * 获取主题
   *
   * @param ch channel
   * @return 主题 or null
   */
  public static String getTopic(String ch) {
    if (ch == null || ch.isEmpty()) {
      return null;
    }
    String[] split = ch.split(DOT);
    if (split.length >= 2) {
      return split[2];
    }
    return null;
  }

  /**
   * 获取深度主题的深度
   *
   * @param ch 深度主题 market.BTC-USDT.depth.step0
   * @return 深度
   */
  public static DepthLevel getDepthLevel(String ch) {
    if (ch == null || ch.isEmpty()) {
      return null;
    }
    String[] split = ch.split(DOT);
    if (split.length >= 2) {
      return DepthLevel.ofName(split[3]);
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
      return "trade".equalsIgnoreCase(split[2]) && "detail".equalsIgnoreCase(split[3]);
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

  /**
   * 生成交易记录channel
   *
   * @param symbol 交易对
   */
  public static String buildTradeDetailChannel(String symbol) {
    return "market." + symbol + ".trade.detail";
  }

  /**
   * 生成市场概要channel
   *
   * @param symbol 交易对
   */
  public static String buildMarketDetailChannel(String symbol) {
    return "market." + symbol + ".detail";
  }

  /**
   * 生成市场深度channel
   *
   * @param symbol 交易对
   * @param level  {@link DepthLevel}
   */
  public static String buildMarketDepthChannel(String symbol, DepthLevel level) {
    return "market." + symbol + ".depth." + level.name();
  }

  /**
   * 生成k线tick channel
   *
   * @param symbol 交易对
   * @param period 时间级别
   */
  public static String buildKLineTickChannel(String symbol, Period period) {
    return "market." + symbol + ".kline." + period.getSymbol();
  }

  /**
   * 解析k线主题
   *
   * @param ch k线主题字符串
   * @return null or {@link KLineChannel}
   */
  public static KLineChannel resolveKLineCh(String ch) {
    if (ch == null || ch.isEmpty()) {
      return null;
    }
    return KLINE_CHANNEL_RESOLVE_CACHE.computeIfAbsent(ch, k -> {
      String[] split = ch.split(DOT);
      if (split.length != 4) {
        // ``market.$symbol.kline.$period``
        return null;
      }
      String symbol = split[1];
      Period period = Period.valueOfSymbol(split[3]);
      if (symbol == null || period == null) {
        return null;
      }
      return new KLineChannel(symbol, period);
    });
  }

  //
  // 解析类
  //
  public static class KLineChannel {
    private String symbol;
    private Period period;

    public KLineChannel(String symbol, Period period) {
      this.symbol = symbol;
      this.period = period;
    }

    public String getSymbol() {
      return symbol;
    }

    public Period getPeriod() {
      return period;
    }
  }
}
