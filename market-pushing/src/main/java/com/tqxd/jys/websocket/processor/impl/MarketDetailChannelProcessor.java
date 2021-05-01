package com.tqxd.jys.websocket.processor.impl;

import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.websocket.processor.ChannelProcessor;
import com.tqxd.jys.websocket.processor.Context;
import com.tqxd.jys.websocket.processor.Response;
import com.tqxd.jys.websocket.session.Session;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

/**
 * 市场概括主题处理器
 *
 * @author lyqingye
 */
@SuppressWarnings("Duplicates")
public class MarketDetailChannelProcessor implements ChannelProcessor {

  @Override
  public boolean doReqIfChannelMatched(Context ctx, String ch, Session session, JsonObject json) {
    if (!ChannelUtil.isMarketDetailChannel(ch)) {
      return false;
    }
    String id = json.getString("id");
    MarketDetailTick tick = ctx.cacheManager().reqMarketDetail(ChannelUtil.getSymbol(ch));
    session.writeText(Json.encode(Response.reqOk(id, ch, tick)));
    return true;
  }

  @Override
  public boolean doSubIfChannelMatched(Context ctx, String sub, Session session, JsonObject json) {
    if (!ChannelUtil.isMarketDetailChannel(sub)) {
      return false;
    }
    String id = json.getString("id");

    // set subscribe
    if (ctx.sessionManager().subscribeChannel(session,sub)) {
      session.writeText(Json.encode(Response.subOK(id, sub)));
    }else{
      session.writeText(Json.encode(Response.err(id,sub,"invalid channel of: " + sub + " server not support!")));
    }
    return true;
  }

  @Override
  public boolean doUnSubIfChannelMatched(Context ctx, String unsub, Session session, JsonObject json) {
    if (!ChannelUtil.isMarketDetailChannel(unsub)) {
      return false;
    }
    String id = json.getString("id");

    // set unsubscribe
    if (ctx.sessionManager().unsubScribeChannel(session,unsub)) {
      session.writeText(Json.encode(Response.subOK(id, unsub)));
    }else{
      session.writeText(Json.encode(Response.err(id,unsub,"invalid channel of: " + unsub + " server not support!")));
    }
    return true;
  }
}
