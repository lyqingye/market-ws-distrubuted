package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdBaseCollector;
import com.tqxd.jys.constance.DataType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class TqxdMatchTradeCollector extends GenericWsCollector {

    private JsonObject config = new JsonObject();

    public TqxdMatchTradeCollector(JsonObject config){
        this.config = config;
    }

    @Override
    public String name() {
        return TqxdMatchTradeCollector.class.getSimpleName();
    }

    @Override
    public String desc() {
        return "天启旭达真实交易记录收集器";
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
                .compose(none-> this.subscribe(DataType.TRADE_DETAIL,config.getString(TqxdBaseCollector.SYMBOL_CONFIG)))
                .onComplete(startPromise);
    }

    @Override
    public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
        Promise<Void> promise = Promise.promise();
        super.subscribe(dataType, symbol, handler);
        promise.future().onSuccess(none ->{

            JsonObject sentData = new JsonObject();
            sentData.put("method","deals.subscribe");
            sentData.put("id",System.currentTimeMillis());
            JsonArray jsonArray = new JsonArray();
            jsonArray.add(config.getString(TqxdBaseCollector.SYMBOL_CONFIG));
            sentData.put("params",jsonArray);

            super.writeText(sentData.toString());
            handler.handle(Future.succeededFuture());
        }).onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
    }
}
