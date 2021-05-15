package com.tqxd.jys.collectors.impl.binance.helper;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.BiAnPeriod;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.depth.DepthTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTickData;
import com.tqxd.jys.utils.ChannelUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 火币数据转换器
 * <p>
 * 自行参考：https://binance-docs.github.io/apidocs/spot/cn/#3c863d56da
 */
public class BiNanceDataConvert {
  /**
   * 数据事件类型
   */
  private static final String KLINE_EVENT = "kline";
  private static final String TRADE_EVENT = "trade";

  /**
   * 通用事件类型
   */
  private static final String EVENT_KEY = "e";
  private static final String SYMBOL_KEY = "s";

  public static void smartConvertTo(JsonObject obj, Function<String, String> deMappingFunc, BiConsumer<DataType, JsonObject> consumer) {
    String eventType = obj.getString(EVENT_KEY);
    if (KLINE_EVENT.equalsIgnoreCase(eventType)) {
      JsonObject data = convertToKLine(obj, deMappingFunc);
      if (data != null) {
        consumer.accept(DataType.KLINE, data);
      }
    } else if (TRADE_EVENT.equalsIgnoreCase(eventType)) {
      JsonObject data = BiNanceDataConvert.convertToTradeDetail(obj, deMappingFunc);
      if (data != null) {
        consumer.accept(DataType.TRADE_DETAIL, data);
      }
    }
  }

  public static JsonObject convertToTradeDetail(JsonObject obj, Function<String, String> deMappingFunc) {
    String symbol = obj.getString(SYMBOL_KEY).toLowerCase();
    TradeDetailTickData tick = new TradeDetailTickData();
    tick.setTradeId(String.valueOf(obj.getLong("t")));
    tick.setId(String.valueOf(obj.getLong("E")));
    tick.setAmount(obj.getString("q"));
    tick.setPrice(obj.getString("p"));
    tick.setTs(obj.getLong("T") / 1000);
    tick.setDirection(obj.getBoolean("m") ? "sell" : "buy");
    String ch = ChannelUtil.buildTradeDetailChannel(deMappingFunc.apply(symbol));
    TradeDetailTick tickList = new TradeDetailTick();
    tickList.setData(Collections.singletonList(tick));
    return JsonObject.mapFrom(TemplatePayload.of(ch, tickList));
  }

  public static JsonObject convertToKLine(JsonObject obj, Function<String, String> deMappingFunc) {
    String symbol = obj.getString(SYMBOL_KEY).toLowerCase();
    JsonObject k = obj.getJsonObject("k");
    KlineTick tick = new KlineTick();
    tick.setId(k.getLong("t") / 1000);
    tick.setAmount(new BigDecimal(k.getString("V")));
    tick.setCount(k.getInteger("n"));
    tick.setOpen(new BigDecimal(k.getString("o")));
    tick.setClose(new BigDecimal(k.getString("c")));
    tick.setLow(new BigDecimal(k.getString("l")));
    tick.setHigh(new BigDecimal(k.getString("h")));
    tick.setVol(new BigDecimal(k.getString("Q")));
    String biAnPeriod = k.getString("i");
    Period period = BiAnPeriod.containsSymbol(biAnPeriod);
    if (null == period) {
      return null;
    }
    String ch = ChannelUtil.buildKLineTickChannel(deMappingFunc.apply(symbol), period);
    return JsonObject.mapFrom(TemplatePayload.of(ch, tick));
  }

  public static JsonObject convertToDepth(JsonObject obj, String symbol) {
    String lastUpdateId = obj.getString("lastUpdateId");
    DepthTick depthTick = new DepthTick();
    depthTick.setAsks(parseBidsOrAsks(obj.getJsonArray("asks")));
    depthTick.setBids(parseBidsOrAsks(obj.getJsonArray("bids")));
    depthTick.setVersion(lastUpdateId);
    depthTick.setTs(System.currentTimeMillis());
    String ch = ChannelUtil.buildMarketDepthChannel(symbol, DepthLevel.step0);
    return JsonObject.mapFrom(TemplatePayload.of(ch, depthTick));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static String[][] parseBidsOrAsks(JsonArray bidsOrAsks) {
    if (bidsOrAsks == null || bidsOrAsks.isEmpty()) {
      return null;
    }
    String[][] result = new String[bidsOrAsks.size()][2];
    List list = bidsOrAsks.getList();
    for (int i = 0; i < list.size(); i++) {
      ArrayList<String> bidOrAsk = (ArrayList<String>) list.get(i);
      if (bidOrAsk.size() == 2) {
        result[i][0] = bidOrAsk.get(0);
        result[i][1] = bidOrAsk.get(1);
      }
    }
    return result;
  }
}
