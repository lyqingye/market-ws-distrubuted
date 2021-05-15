package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdCollector;
import com.tqxd.jys.collectors.impl.tqxd.helper.TqxdRequestUtils;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.Period;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TqxdKlineCollector extends GenericWsCollector {
  public static final String PERIOD_CONFIG = "period";
  private static final Logger log = LoggerFactory.getLogger(TqxdKlineCollector.class);
  private JsonObject config;

  public TqxdKlineCollector(JsonObject config) {
    this.config = config;
  }

  @Override
  public String name() {
    return TqxdKlineCollector.class.getSimpleName();
  }

  @Override
  public String desc() {
    return "天启旭达k线收集器";
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
        .compose(none -> this.subscribe(DataType.KLINE, config.getString(TqxdCollector.SYMBOL_CONFIG)))
        .onComplete(startPromise);
  }

  @Override
  public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future()
        .onSuccess(none -> {
          Period period = Period.valueOfName(config.getString(PERIOD_CONFIG));
          if (period != null) {
            super.writeText(TqxdRequestUtils.buildSubscribeKLineReq(System.currentTimeMillis() / 1000, TqxdRequestUtils.toTqxdSymbol(symbol), period));
          }
          handler.handle(Future.succeededFuture());
        })
        .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  @Override
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    if (frame.isText() && frame.isFinal()) {
//            unParkReceives(DataType.KLINE,new JsonObject(frame.textData()));
      log.info("[Tqxd Kline] subscribe result:{}", frame.textData());
    }
  }
}
