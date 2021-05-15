package com.tqxd.jys.collectors.impl.binance.sub;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.constance.DataType;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 作为币安子收集器，用于收集深度，之所以这么设计是因为币安深度需要根据路径来订阅
 * 所以一个订阅对应一个websocket 客户端连接
 */
public class BiNanceDepthCollector extends GenericWsCollector {
    private static final Logger log = LoggerFactory.getLogger(BiNanceDepthCollector.class);
    private JsonObject config;

    public BiNanceDepthCollector(JsonObject config) {
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public String name() {
        return BiNanceDepthCollector.class.getSimpleName();
    }

    @Override
    public String desc() {
        return "币安深度收集器";
    }

    @Override
    public JsonObject config() {
        return config;
    }

    @Override
    public void onFrame(WebSocket client, WebSocketFrame frame) {
        if (frame.isPing()) {
            log.info("[BiNance Depth] ping:{}", frame.textData());
            super.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
        } else if (frame.isText() && frame.isFinal()) {
            JsonObject obj = new JsonObject(frame.textData());
            unParkReceives(DataType.DEPTH, obj);
        } else {
            log.warn("[BiNance Depth] unknown message from: {}, frameType: {} content: {}", this.name(), frame.type().name(), frame.binaryData().toString());
        }
    }
}
