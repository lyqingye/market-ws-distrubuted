package com.tqxd.jys.collectors.impl.bian;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.BiAnPeriod;
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
import java.util.Map;

public class BiAnDataConvert {

    public static JsonObject trade(JsonObject obj, Map<String, String> symbolDeMapping) {
        String trade = obj.getString("e");
        String symbol = obj.getString("s");
        TradeDetailTickData tick = new TradeDetailTickData();
        tick.setTradeId(String.valueOf(obj.getLong("t")));
        tick.setId(String.valueOf(obj.getLong("E")));
        tick.setAmount(obj.getString("q"));
        tick.setPrice(obj.getString("p"));
        tick.setTs(obj.getLong("T") / 1000);
        tick.setDirection(obj.getBoolean("m") ? "sell" : "buy");
        String ch = ChannelUtil.buildMarketDetailChannel(symbolDeMapping.get(trade + symbol));
        TradeDetailTick tickList = new TradeDetailTick();
        tickList.setData(Collections.singletonList(tick));
        return JsonObject.mapFrom(TemplatePayload.of(ch, tickList));
    }

    public static JsonObject kline(JsonObject obj,Map<String, String> symbolDeMapping) {
        String kline = obj.getString("e");
        String symbol = obj.getString("s");
        JsonObject k = obj.getJsonObject("k");
        KlineTick tick = new KlineTick();
        tick.setId(k.getLong("t"));
        tick.setAmount(new BigDecimal(k.getString("V")));
        tick.setCount(Integer.valueOf(k.getString("n")));
        tick.setOpen(new BigDecimal(k.getString("o")));
        tick.setClose(new BigDecimal(k.getString("c")));
        tick.setLow(new BigDecimal(k.getString("l")));
        tick.setHigh(new BigDecimal(k.getString("h")));
        tick.setVol(new BigDecimal(k.getString("Q")));
        String biAnPeriod = k.getString("i");
        Period period = BiAnPeriod.containsSymbol(biAnPeriod);
        if(null == period){
            return null;
        }
        String ch = ChannelUtil.buildKLineTickChannel(symbolDeMapping.get(kline + symbol + biAnPeriod), period);
        return JsonObject.mapFrom(TemplatePayload.of(ch, tick));
    }

    public static JsonObject depth(JsonObject obj,String symbol) {
        String lastUpdateId = obj.getString("lastUpdateId");
        JsonArray bids = obj.getJsonArray("bids");
        JsonArray asks = obj.getJsonArray("asks");


        DepthTick depthTick = new DepthTick();
        depthTick.setAsks(null);
        depthTick.setBids(null);
        depthTick.setVersion(lastUpdateId);
        String ch = ChannelUtil.buildMarketDepthChannel(symbol, DepthLevel.step0);
        return JsonObject.mapFrom(TemplatePayload.of(ch, depthTick));
    }


}
