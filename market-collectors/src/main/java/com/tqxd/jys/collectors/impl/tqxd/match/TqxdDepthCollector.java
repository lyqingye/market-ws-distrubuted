package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdCollector;
import com.tqxd.jys.collectors.impl.tqxd.helper.TqxdRequestUtils;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.DepthLevel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class TqxdDepthCollector extends GenericWsCollector {
  private static final Logger log = LoggerFactory.getLogger(TqxdDepthCollector.class);
  private JsonObject config;

  public TqxdDepthCollector(JsonObject config) {
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    Promise<Void> promise = Promise.promise();
    super.start(promise);
    promise.future()
        .compose(none -> this.subscribe(DataType.DEPTH, config.getString(TqxdCollector.SYMBOL_CONFIG)))
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
    promise.future()
        .onSuccess(none -> {
          super.writeText(TqxdRequestUtils.buildSubscribeDepthReq(System.currentTimeMillis() / 1000, TqxdRequestUtils.toTqxdSymbol(symbol), 20, DepthLevel.step0));
          handler.handle(Future.succeededFuture());
        })
        .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  @Override
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    if (frame.isText() && frame.isFinal()) {
//            unParkReceives(DataType.DEPTH,new JsonObject(frame.textData()));
      log.info("[Tqxd Depth] result:{}", frame.textData());
    }
  }


}
