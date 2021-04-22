package com.tqxd.jys.websocket.adapter.huobi.cmd.impl.kline;

import com.tqxd.jys.utils.HuoBiUtils;
import com.tqxd.jys.websocket.adapter.Cmd;
import com.tqxd.jys.websocket.context.PushingContext;
import com.tqxd.jys.websocket.session.WsSession;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * @author yjt
 * @since 2020/11/14 18:28
 */
public class TradeDetailSubCmd implements Cmd {
    @Override
    public boolean canExecute(JsonObject json) {
        return HuoBiUtils.isTradeDetailSubscribeReq(json);
    }

    @Override
    public void execute(JsonObject json, PushingContext ctx, WsSession curSession) {
        String sub = json.getString("sub");
        Buffer buffer = ctx.getLatestTradeBufferCache().get(sub);
        curSession.subTradeDetail(sub);
        if (buffer != null) {
            curSession.getSocket().write(buffer);
        }
    }
}
