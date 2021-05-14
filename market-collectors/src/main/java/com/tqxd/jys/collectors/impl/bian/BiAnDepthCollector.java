package com.tqxd.jys.collectors.impl.bian;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.constance.DataType;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class BiAnDepthCollector extends GenericWsCollector {
    private static final Logger log = LoggerFactory.getLogger(BiAnDepthCollector.class);
    private JsonObject config;

    public BiAnDepthCollector (JsonObject config) {
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public String name() {
        return BiAnDepthCollector.class.getSimpleName();
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
        if(frame.isPing()){
            log.info("[BiAn Depth] ping:{}",frame.textData());
            super.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
            return;
        }
        if (frame.isText() && frame.isFinal()) {
            JsonObject obj = new JsonObject(frame.textData());
            unParkReceives(DataType.DEPTH,obj);
            log.info("[BiAn Depth] subscribe result:{}",frame.textData());
        }
    }
}
