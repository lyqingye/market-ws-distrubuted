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

public class TqxdMatchKlineCollector extends GenericWsCollector {

    private static final Logger log = LoggerFactory.getLogger(TqxdMatchKlineCollector.class);

    private JsonObject config;

    public TqxdMatchKlineCollector(JsonObject config){
        this.config = config;
    }

    @Override
    public String name() {
        return TqxdMatchKlineCollector.class.getSimpleName();
    }

    @Override
    public String desc() {
        return "天启旭达真实k线收集器";
    }

    @Override
    public JsonObject config() {
        return config;
    }


    @Override
    public synchronized void start(Promise<Void> startPromise) throws Exception {
        Promise<Void> promise = Promise.promise();
        super.start(startPromise);
        promise.future()
                .compose(none -> this.subscribe(DataType.KLINE,config.getString(TqxdBaseCollector.SYMBOL_CONFIG)))
                .onComplete(startPromise);
    }

    @Override
    public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
        Promise<Void> promise = Promise.promise();
        super.subscribe(dataType, symbol, handler);
        promise.future()
                .onSuccess(none ->{

                    JsonObject sentData = new JsonObject();
                    sentData.put("method","kline.subscribe");
                    sentData.put("id",System.currentTimeMillis());
                    JsonArray jsonArray = new JsonArray();
                    jsonArray.add(config.getString(TqxdBaseCollector.SYMBOL_CONFIG));
                    jsonArray.add(config.getInteger(TqxdBaseCollector.INTERVAL_CONFIG));
                    sentData.put("params",jsonArray);
                    super.writeText(sentData.toString());

                    handler.handle(Future.succeededFuture());
                })
                .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
    }

    @Override
    public void onFrame(WebSocket client, WebSocketFrame frame) {
        if(frame.isPing()){
            log.info("[Tqxd Kline] ping:{}",frame.textData());
            super.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
            return;
        }
        if(frame.isText() && frame.isFinal()){
            unParkReceives(DataType.KLINE,new JsonObject(frame.textData()));
            log.info("[Tqxd Kline] subscribe result:{}",frame.textData());
        }
    }
}
