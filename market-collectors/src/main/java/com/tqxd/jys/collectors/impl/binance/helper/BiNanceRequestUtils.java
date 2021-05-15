package com.tqxd.jys.collectors.impl.binance.helper;

import com.tqxd.jys.constance.BiAnPeriod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class BiNanceRequestUtils {

  public static String buildSubscribeKLineReq(long subscribeId, String symbol) {
    return buildSubscribeReq(subscribeId, buildSubscribeKLineParams(symbol));
  }

  public static String buildUnSubscribeKLineReq(long subscribeId, String symbol) {
    return buildUnSubscribeReq(subscribeId, buildSubscribeKLineParams(symbol));
  }

  public static String buildSubscribeTradeDetailReq(long subscribeId, String symbol) {
    return buildSubscribeReq(subscribeId, buildSubscribeTradeDetailParams(symbol));
  }

  public static String buildUnSubscribeTradeDetailReq(long subscribeId, String symbol) {
    return buildUnSubscribeReq(subscribeId, buildSubscribeTradeDetailParams(symbol));
  }

  private static JsonArray buildSubscribeKLineParams(String symbol) {
    JsonArray params = new JsonArray();
    for (BiAnPeriod period : BiAnPeriod.values()) {
      //<symbol>@kline_<interval>
      String symbolPeriod = String.format("%s@kline_%s", symbol, period.getSymbol());
      params.add(symbolPeriod);
    }
    return params;
  }

  private static JsonArray buildSubscribeTradeDetailParams(String symbol) {
    JsonArray params = new JsonArray();
    params.add(String.format("%s@trade", symbol));
    return params;
  }

  private static String buildSubscribeReq(long subscribeId, JsonArray params) {
    JsonObject json = new JsonObject();
    json.put("method", "SUBSCRIBE");
    json.put("id", subscribeId);
    json.put("params", params);
    return json.toString();
  }

  private static String buildUnSubscribeReq(long subscribeId, JsonArray params) {
    JsonObject json = new JsonObject();
    json.put("method", "UNSUBSCRIBE");
    json.put("id", subscribeId);
    json.put("params", params);
    return json.toString();
  }
}
