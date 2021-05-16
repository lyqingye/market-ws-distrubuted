package com.tqxd.jys.collectors.impl.huobi.helper;

import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author yjt
 * @since 2020/9/28 下午2:06
 */
public final class HuoBiUtils {

  public static String buildKLIneChannel(String symbolId, Period period) {
    return String.format("market.%s.kline.%s", symbolId, period.getSymbol());
  }

  public static String buildDepthChannel(String symbolId, DepthLevel depth) {
    return String.format("market.%s.depth.%s", symbolId, depth.name());
  }

  public static String buildTradeDetailChannel(String symbolId) {
    return String.format("market.%s.trade.detail", symbolId);
  }

  public static String buildKLineSubReq(long id, String channel) {
    JsonObject json = new JsonObject();
    json.put("sub", channel);
    json.put("id", String.valueOf(id));
    return json.toString();
  }

  public static String buildDepthSubReq(long id, String channel, int asksLimit, int bidsLimit) {
    JsonObject json = new JsonObject();
    json.put("sub", channel);
    json.put("id", String.valueOf(id));
    json.put("pick", new JsonArray().add("bids." + bidsLimit).add("asks." + asksLimit));
    return json.toString();
  }

  public static String buildTradeDetailSubReq(long id, String channel) {
    JsonObject json = new JsonObject();
    json.put("sub", channel);
    json.put("id", String.valueOf(id));
    return json.toString();
  }

  public static String buildKLineUnsubReq(long id, String symbolId, Period period) {
    JsonObject json = new JsonObject();
    json.put("unsub", buildKLIneChannel(symbolId, period));
    json.put("id", String.valueOf(id));
    return json.toString();
  }

  public static String buildDepthUnsubReq(long id, String symbolId, DepthLevel depth, int asksLimit, int bidsLimit) {
    JsonObject json = new JsonObject();
    json.put("unsub", buildDepthChannel(symbolId, depth));
    json.put("id", String.valueOf(id));
    json.put("pick", new JsonArray().add("bids." + bidsLimit).add("asks." + asksLimit));
    return json.toString();
  }

  public static String buildTradeDetailUnsubReq(long id, String symbolId) {
    JsonObject json = new JsonObject();
    json.put("unsub", buildTradeDetailChannel(symbolId));
    json.put("id", String.valueOf(id));
    return json.toString();
  }
}
