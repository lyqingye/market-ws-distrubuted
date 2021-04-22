package com.tqxd.jys.websocket.adapter.huobi.cmd.impl.kline;

import com.tqxd.jys.utils.HuoBiUtils;
import com.tqxd.jys.websocket.adapter.Cmd;
import com.tqxd.jys.websocket.adapter.huobi.payload.req.HBSubReq;
import com.tqxd.jys.websocket.context.PushingContext;
import com.tqxd.jys.websocket.session.WsSession;
import io.vertx.core.json.JsonObject;

/**
 * @author yjt
 * @since 2020/11/14 18:26
 */
public class KlineSubCmd implements Cmd {

  private HBSubReq req;

  @Override
  public boolean canExecute(JsonObject json) {
    return HuoBiUtils.isKlineSubscribeReq(json);
  }

  @Override
  public void execute(JsonObject json, PushingContext ctx, WsSession curSession) {
    curSession.subKline(json.getString("sub"));
    // k线会话分区
    ctx.getKlineSM().put(curSession.getSocket().textHandlerID(), curSession);
  }
}
