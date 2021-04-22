package com.tqxd.jys.websocket.adapter;

import com.tqxd.jys.websocket.context.PushingContext;
import com.tqxd.jys.websocket.session.WsSession;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author yjt
 * @since 2020/11/14 18:21
 */
public interface Cmd {

    boolean canExecute(JsonObject json);

    default boolean canExecute(JsonArray jsonArray) {
        return false;
    }

    default void execute(JsonArray jsonArray, PushingContext ctx, WsSession curSession) {
        throw new UnsupportedOperationException();
    }

    void execute(JsonObject json, PushingContext ctx, WsSession curSession);
}
