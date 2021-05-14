package com.tqxd.jys.utils;

import com.tqxd.jys.constance.BiAnPeriod;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import io.vertx.core.json.JsonObject;

/**
 * @author hwh
 * @since 2020/05/10 下午2:06
 */
public final class TqxdUtils {

  public static boolean isPongReq(JsonObject obj) {
    if (obj == null) {
      return false;
    }
    return obj.containsKey("pong");
  }

  public static boolean isSubscribeReq(JsonObject obj) {
    if (obj == null) {
      return false;
    }

    return obj.containsKey("sub");
  }

  public static boolean isDepthSubscribeReq(JsonObject obj) {
    if (isSubscribeReq(obj)) {
      String sub = obj.getString("sub");

      if (sub != null) {
        return sub.contains("depth");
      }
    }
    return false;
  }

  public static boolean isDepthUnSubscribeReq(JsonObject obj) {
    if (isUnSubscribeReq(obj)) {
      String sub = obj.getString("unsub");

      if (sub != null) {
        return sub.contains("depth");
      }
    }
    return false;
  }

  public static boolean isKlineUnSubscribeReq(JsonObject obj) {
    if (isUnSubscribeReq(obj)) {
      String sub = obj.getString("unsub");

      if (sub != null) {
        return sub.contains("kline");
      }
    }
    return false;
  }

  public static boolean isDetailSubscribeReq(JsonObject obj) {
    if (isSubscribeReq(obj)) {
      String sub = obj.getString("sub");

      String[] split = sub.split("\\.");

      if (split.length != 3) {
        return false;
      }
      return "detail".equals(split[2]);
    }
    return false;
  }

  public static boolean isTradeDetailSubscribeReq(JsonObject obj) {
    if (isSubscribeReq(obj)) {
      String sub = obj.getString("sub");

      String[] split = sub.split("\\.");

      if (split.length != 4) {
        return false;
      }
      return "trade".equals(split[2]) && "detail".equals(split[3]);
    }
    return false;
  }

  public static boolean isDetailUnSubscribeReq(JsonObject obj) {
    if (isUnSubscribeReq(obj)) {
      String sub = obj.getString("unsub");

      String[] split = sub.split("\\.");

      if (split.length != 3) {
        return false;
      }
      return "detail".equals(split[2]);
    }
    return false;
  }


  public static boolean isUnSubscribeReq(JsonObject obj) {
    if (obj == null) {
      return false;
    }

    return obj.containsKey("unsub");
  }

  public static boolean isPullHistoryReq(JsonObject obj) {
    if (obj == null) {
      return false;
    }

    return obj.containsKey("req") &&
        obj.containsKey("from") &&
        obj.containsKey("to");
  }

  public static String getSymbolFromKlineSub(String sub) {
    String[] split = sub.split("\\.");

    if (split.length != 4) {
      return null;
    }
    return split[1];
  }

  public static String toKlineSub(String symbolId, Period period) {
    return String.format("%s@kline_%s", symbolId, period.getSymbol());
  }

  public static String toKlineSymbolDeMappingKey(String symbolId, Period period) {
    return String.format("kline%s%s", toGenericSymbolUpperCase(symbolId), period.getSymbol());
  }

  private static String toGenericSymbolUpperCase(String symbol) {
    return symbol.replace("-", "")
            .replace("/", "")
            .toUpperCase();
  }

  public static String toDepthSub(String symbolId, DepthLevel depth) {
    return String.format("%s@depth%s", symbolId, depth.name());
  }

  public static String toDetailSub(String symbolId) {
    return String.format("market.%s.detail", symbolId);
  }

  public static String toTradeDetailSub(String symbolId) {
    return String.format("%s@aggTrade", symbolId);
  }


  public static String toSymbol(String symbol) {
    return symbol.replace("-", "")
        .replace("/", "")
        .toLowerCase();
  }
}
