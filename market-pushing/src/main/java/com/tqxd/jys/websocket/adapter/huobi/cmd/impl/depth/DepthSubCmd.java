package com.tqxd.jys.websocket.adapter.huobi.cmd.impl.depth;

import com.tqxd.jys.utils.HuoBiUtils;
import com.tqxd.jys.websocket.adapter.Cmd;
import com.tqxd.jys.websocket.context.PushingContext;
import com.tqxd.jys.websocket.session.WsSession;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class DepthSubCmd implements Cmd {
    @Override
    public boolean canExecute(JsonObject json) {
        return HuoBiUtils.isDepthSubscribeReq(json);
    }

    @Override
    public void execute(JsonObject json, PushingContext ctx, WsSession curSession) {
        String sub = json.getString("sub");
        curSession.subDepth(sub);
        ctx.getDepthSM().put(curSession.getSocket().textHandlerID(), curSession);
        // 发送历史数据
        Buffer buffer = ctx.getDepthChartCache().get(sub);
        if (buffer != null) {
            curSession.getSocket().write(buffer);
        }
    }
}
