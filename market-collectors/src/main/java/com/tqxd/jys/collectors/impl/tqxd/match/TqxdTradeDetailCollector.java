package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdCollector;
import com.tqxd.jys.collectors.impl.tqxd.helper.TqxdRequestUtils;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTickData;
import com.tqxd.jys.utils.ChannelUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TqxdTradeDetailCollector extends GenericWsCollector {
  private static final int DATA_INDEX = 1;
  private static final String ID_KEY = "id";
  private static final String TIME_KEY = "time";
  private static final String PRICE_KEY = "price";
  private static final String AMOUNT_KEY = "amount";
  private static final String TYPE_KEY = "type";
  private static final Logger log = LoggerFactory.getLogger(TqxdTradeDetailCollector.class);
  private JsonObject config;

  public TqxdTradeDetailCollector(JsonObject config) {
    this.config = config;
  }

  @Override
  public String name() {
    return TqxdTradeDetailCollector.class.getSimpleName();
  }

  @Override
  public String desc() {
    return "天启旭达交易记录收集器";
  }

  @Override
  public JsonObject config() {
    return config;
  }

  @Override
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    Promise<Void> promise = Promise.promise();
    super.start(promise);
    promise.future()
        .compose(none -> this.subscribe(DataType.TRADE_DETAIL, config.getString(TqxdCollector.SYMBOL_CONFIG)))
        .onComplete(startPromise);
  }

  @Override
  public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future().onSuccess(none -> {
      super.writeText(TqxdRequestUtils.buildSubscribeTradeDetailReq(System.currentTimeMillis() / 1000, TqxdRequestUtils.toTqxdSymbol(symbol)));
      handler.handle(Future.succeededFuture());
    }).onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    if (frame.isText() && frame.isFinal()) {
      JsonObject obj = (JsonObject) Json.decodeValue(frame.textData());
      if (TqxdRequestUtils.TRADE_DETAIL_UPDATE_RESP.equalsIgnoreCase(obj.getString(TqxdRequestUtils.METHOD))) {
        JsonArray params = obj.getJsonArray(TqxdRequestUtils.PARAMS);
        ArrayList<Map> traderList = (ArrayList<Map>) params.getList().get(DATA_INDEX);
        if (traderList == null || traderList.isEmpty()) {
          return;
        }
        TradeDetailTick tick = new TradeDetailTick();
        List<TradeDetailTickData> dataList = new ArrayList<>(traderList.size());
        for (Map o : traderList) {
          dataList.add(convertToData(o));
        }
        tick.setData(dataList);
        String ch = ChannelUtil.buildTradeDetailChannel(config.getString(TqxdCollector.SYMBOL_CONFIG));
        TemplatePayload<TradeDetailTick> payload = TemplatePayload.of(ch, tick);
        unParkReceives(DataType.TRADE_DETAIL, JsonObject.mapFrom(payload));
      } else {
        log.warn("[TqxdTradeDetail]-{}: receive ignored data is: {}", config.getString(TqxdCollector.SYMBOL_CONFIG), frame.textData());
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  private TradeDetailTickData convertToData(Map map) {
    TradeDetailTickData data = new TradeDetailTickData();
    Double time = (Double) map.get(TIME_KEY);
    data.setTs(time.longValue());
    data.setId(String.valueOf(map.get(ID_KEY)));
    data.setDirection((String) map.get(TYPE_KEY));
    data.setAmount((String) map.get(AMOUNT_KEY));
    data.setPrice((String) map.get(PRICE_KEY));
    return data;
  }
}
