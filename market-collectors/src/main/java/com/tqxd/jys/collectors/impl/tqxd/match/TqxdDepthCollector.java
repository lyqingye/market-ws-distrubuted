package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdCollector;
import com.tqxd.jys.collectors.impl.tqxd.helper.TqxdRequestUtils;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.messagebus.payload.depth.DepthTick;
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
import java.util.Map;
import java.util.Objects;

public class TqxdDepthCollector extends GenericWsCollector {
  private static final int DATA_INDEX = 1;
  private static final String ASKS_KEY = "asks";
  private static final String BIDS_KEY = "bids";
  private static final Logger log = LoggerFactory.getLogger(TqxdDepthCollector.class);
  private JsonObject config;

  public TqxdDepthCollector(JsonObject config) {
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    Promise<Void> promise = Promise.promise();
    super.start(promise);
    promise.future().compose(none -> this.subscribe(DataType.DEPTH, config.getString(TqxdCollector.SYMBOL_CONFIG)))
        .onComplete(startPromise);
  }

  @Override
  public String name() {
    return TqxdDepthCollector.class.getSimpleName();
  }

  @Override
  public JsonObject config() {
    return config;
  }

  @Override
  public String desc() {
    return "天启旭达深度收集器";
  }

  @Override
  public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future().onSuccess(none -> {
      super.writeText(TqxdRequestUtils.buildSubscribeDepthReq(System.currentTimeMillis() / 1000,
          TqxdRequestUtils.toTqxdSymbol(symbol), 20, DepthLevel.step0));
      handler.handle(Future.succeededFuture());
    }).onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    if (frame.isText() && frame.isFinal()) {
      JsonObject obj = (JsonObject) Json.decodeValue(frame.textData());
      if (TqxdRequestUtils.DEPTH_UPDATE_RESP.equalsIgnoreCase(obj.getString(TqxdRequestUtils.METHOD))) {
        JsonArray params = obj.getJsonArray(TqxdRequestUtils.PARAMS);
        if (params.size() >= 1) {
          Object data = params.getList().get(DATA_INDEX);
          if (data instanceof Map) {
            Map<String, ArrayList<ArrayList<String>>> map = (Map<String, ArrayList<ArrayList<String>>>) data;
            ArrayList<ArrayList<String>> asks = map.get(ASKS_KEY);
            ArrayList<ArrayList<String>> bids = map.get(BIDS_KEY);

            DepthTick tick = new DepthTick();
            tick.setTs(System.currentTimeMillis());
            tick.setAsks(flatAsksOrBids(asks));
            tick.setBids(flatAsksOrBids(bids));
            String ch = ChannelUtil.buildMarketDepthChannel(config.getString(TqxdCollector.SYMBOL_CONFIG),
                DepthLevel.step0);
            TemplatePayload<DepthTick> payload = TemplatePayload.of(ch, tick);
            unParkReceives(DataType.DEPTH, JsonObject.mapFrom(payload));
          }
        }
      } else {
        log.warn("[TqxdDepth]-{}: receive ignored data is: {}", config.getString(TqxdCollector.SYMBOL_CONFIG),
            frame.textData());
      }
    }
  }

  private String[][] flatAsksOrBids(ArrayList<ArrayList<String>> asksOrBids) {
    if (asksOrBids == null || asksOrBids.isEmpty()) {
      return null;
    }
    String[][] result = new String[asksOrBids.size()][2];
    for (int i = 0; i < asksOrBids.size(); i++) {
      ArrayList<String> askOrBid = asksOrBids.get(i);
      result[i][0] = askOrBid.get(0);
      result[i][1] = askOrBid.get(1);
    }
    return result;
  }
}
