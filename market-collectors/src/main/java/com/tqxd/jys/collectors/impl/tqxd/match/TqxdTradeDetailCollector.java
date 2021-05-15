package com.tqxd.jys.collectors.impl.tqxd.match;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdCollector;
import com.tqxd.jys.collectors.impl.tqxd.helper.TqxdRequestUtils;
import com.tqxd.jys.constance.DataType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TqxdTradeDetailCollector extends GenericWsCollector {
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
        super.start(startPromise);
        promise.future()
            .compose(none -> this.subscribe(DataType.TRADE_DETAIL, config.getString(TqxdCollector.SYMBOL_CONFIG)))
                .onComplete(startPromise);
    }

    @Override
    public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
        Promise<Void> promise = Promise.promise();
        super.subscribe(dataType, symbol, handler);
        promise.future().onSuccess(none -> {
            super.writeText(TqxdRequestUtils.buildSubscribeTradeDetailReq(System.currentTimeMillis() / 1000, TqxdRequestUtils.toTqxdSymbol(symbol)));
            handler.handle(Future.succeededFuture());
        }).onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
    }

    @Override
    public void onFrame(WebSocket client, WebSocketFrame frame) {
        if (frame.isText() && frame.isFinal()) {
//            unParkReceives(DataType.KLINE,new JsonObject(frame.textData()));
            log.info("[Tqxd Kline] subscribe result:{}", frame.textData());
        }
    }
}
