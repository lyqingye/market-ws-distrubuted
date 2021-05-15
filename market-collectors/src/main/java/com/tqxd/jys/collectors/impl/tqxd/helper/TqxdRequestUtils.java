package com.tqxd.jys.collectors.impl.tqxd.helper;

import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class TqxdRequestUtils {
  private static final String KLINE_SUBSCRIBE_METHOD = "kline.subscribe";
  private static final String KLINE_UNSUBSCRIBE_METHOD = "kline.unsubscribe";
  private static final String DEPTH_SUBSCRIBE_METHOD = "depth.subscribe";
  private static final String DEPTH_UNSUBSCRIBE_METHOD = "depth.unsubscribe";
  private static final String TRADE_DETAIL_SUBSCRIBE_METHOD = "deals.subscribe";
  private static final String TRADE_DETAIL_UNSUBSCRIBE_METHOD = "deals.unsubscribe";
  private static final Map<DepthLevel, String> DEPTH_LEVEL_MAPPING = new HashMap<>();

  static {
    DEPTH_LEVEL_MAPPING.put(DepthLevel.step0, "0");
    DEPTH_LEVEL_MAPPING.put(DepthLevel.step1, "1");
    DEPTH_LEVEL_MAPPING.put(DepthLevel.step2, "2");
    DEPTH_LEVEL_MAPPING.put(DepthLevel.step3, "3");
    DEPTH_LEVEL_MAPPING.put(DepthLevel.step4, "4");
    DEPTH_LEVEL_MAPPING.put(DepthLevel.step5, "5");
  }

  public static String toTqxdSymbol(String symbol) {
    return symbol.replace("-", "")
        .replace("/", "")
        .toUpperCase();
  }

  public static String buildSubscribeKLineReq(long subscribeId, String symbol, Period period) {
    return buildRequest(subscribeId, KLINE_SUBSCRIBE_METHOD, buildSubscribeKlineParams(symbol, period.getMill()));
  }

  public static String buildUnSubscribeKLineReq(long subscribeId, String symbol, Period period) {
    return buildRequest(subscribeId, KLINE_UNSUBSCRIBE_METHOD, buildSubscribeKlineParams(symbol, period.getMill()));
  }

  public static String buildSubscribeDepthReq(long subscribeId, String symbol, int limit, DepthLevel level) {
    return buildRequest(subscribeId, DEPTH_SUBSCRIBE_METHOD, buildSubscribeDepthParams(symbol, limit, level));
  }

  public static String buildUnSubscribeDepthReq(long subscribeId, String symbol, int limit, DepthLevel level) {
    return buildRequest(subscribeId, DEPTH_UNSUBSCRIBE_METHOD, buildSubscribeDepthParams(symbol, limit, level));
  }

  public static String buildSubscribeTradeDetailReq(long subscribeId, String symbol) {
    return buildRequest(subscribeId, TRADE_DETAIL_SUBSCRIBE_METHOD, buildSubscribeTradeDetailParams(symbol));
  }

  public static String buildUnSubscribeTradeDetailReq(long subscribeId, String symbol) {
    return buildRequest(subscribeId, TRADE_DETAIL_UNSUBSCRIBE_METHOD, buildSubscribeTradeDetailParams(symbol));
  }

  private static JsonArray buildSubscribeKlineParams(String symbol, long periodSec) {
    JsonArray params = new JsonArray();
    params.add(symbol);
    params.add(periodSec);
    return params;
  }

  private static JsonArray buildSubscribeDepthParams(String symbol, int limit, DepthLevel level) {
    JsonArray params = new JsonArray();
    params.add(symbol);
    params.add(limit);
    params.add(DEPTH_LEVEL_MAPPING.get(level));
    return params;
  }

  private static JsonArray buildSubscribeTradeDetailParams(String symbol) {
    JsonArray params = new JsonArray();
    params.add(symbol);
    return params;
  }

  private static String buildRequest(long subscribeId, String method, JsonArray params) {
    JsonObject json = new JsonObject();
    json.put("method", method);
    json.put("id", subscribeId);
    json.put("params", params);
    return json.toString();
  }
}
