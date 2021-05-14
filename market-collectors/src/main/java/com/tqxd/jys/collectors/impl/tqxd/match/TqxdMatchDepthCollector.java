package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdBaseCollector;
import com.tqxd.jys.constance.DataType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class TqxdMatchDepthCollector extends GenericWsCollector {

    private static final Logger log = LoggerFactory.getLogger(TqxdMatchDepthCollector.class);
    private JsonObject config;


    public TqxdMatchDepthCollector(JsonObject config) {
        this.config = Objects.requireNonNull(config);
    }


    @Override
    public synchronized void start(Promise<Void> startPromise) throws Exception {
        Promise<Void> promise = Promise.promise();
        super.start(promise);
        promise.future()
                .compose(none -> this.subscribe(DataType.DEPTH, config.getString(TqxdBaseCollector.SYMBOL_CONFIG)))
                .onComplete(startPromise);
    }

    @Override
    public String name() {
        return TqxdMatchDepthCollector.class.getSimpleName();
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

                    String theSymbol = config.getString(TqxdBaseCollector.SYMBOL_CONFIG);
                    JsonObject sentData = new JsonObject();
                    sentData.put("method","depth.subscribe");
                    sentData.put("id",System.currentTimeMillis());
                    JsonArray jsonArray = new JsonArray();
                    jsonArray.add(theSymbol);
                    jsonArray.add(5);
                    jsonArray.add(0);
                    sentData.put("params",jsonArray);

                    super.writeText(sentData.toString());
                    handler.handle(Future.succeededFuture());
                })
                .onFailure(throwable -> {
                    handler.handle(Future.failedFuture(throwable));
                });
    }

    @Override
    public void onFrame(WebSocket client, WebSocketFrame frame) {
        if(frame.isPing()){
            log.info("[Tqxd Depth] ping:{}",frame.textData());
            super.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
            return;
        }
        if (frame.isText() && frame.isFinal()) {
            unParkReceives(DataType.DEPTH,new JsonObject(frame.textData()));
            log.info("[Tqxd Depth] subscribe result:{}",frame.textData());
        }
    }


}
