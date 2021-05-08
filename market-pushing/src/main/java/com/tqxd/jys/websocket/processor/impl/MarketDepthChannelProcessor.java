package com.tqxd.jys.websocket.processor.impl;

import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.messagebus.payload.depth.DepthTick;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.websocket.cache.CacheManager;
import com.tqxd.jys.websocket.processor.ChannelProcessor;
import com.tqxd.jys.websocket.session.Session;
import com.tqxd.jys.websocket.session.SessionManager;
import com.tqxd.jys.websocket.transport.Response;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

/**
 * 市场深度主题处理器
 *
 * @author lyqingye
 */
public class MarketDepthChannelProcessor implements ChannelProcessor {
  private CacheManager cacheManager;
  private SessionManager sessionManager;

  public MarketDepthChannelProcessor(CacheManager cacheManager, SessionManager sessionManager) {
    this.cacheManager = Objects.requireNonNull(cacheManager);
    this.sessionManager = Objects.requireNonNull(sessionManager);
  }

  @Override
  public boolean doReqIfChannelMatched(String ch, Session session, JsonObject json) {
    if (!ChannelUtil.isDepthChannel(ch)) {
      return false;
    }
    String id = json.getString("id");
    DepthLevel depthLevel = ChannelUtil.getDepthLevel(ch);
    if (depthLevel == null) {
      session.writeText(Json.encode(Response.err(id, ch, "invalid depth channel string! format: market.$symbol.depth.$level, ${level} = [step0,step1...step5]")));
      return true;
    }

    DepthTick tick = cacheManager.reqDepth(ChannelUtil.getSymbol(ch), depthLevel, -1);
    session.writeText(Json.encode(Response.reqOk(id, ch, tick)));
    return true;
  }

  @Override
  public boolean doSubIfChannelMatched(String sub, Session session, JsonObject json) {
    if (!ChannelUtil.isDepthChannel(sub)) {
      return false;
    }
    String id = json.getString("id");

    // set subscribe
    if (sessionManager.subscribeChannel(session, sub)) {
      session.writeText(Json.encode(Response.subOK(id, sub)));
    } else {
      session.writeText(Json.encode(Response.err(id, sub, "invalid channel of: " + sub + " server not support!")));
    }
    return true;
  }

  @Override
  public boolean doUnSubIfChannelMatched(String unsub, Session session, JsonObject json) {
    if (!ChannelUtil.isDepthChannel(unsub)) {
      return false;
    }
    String id = json.getString("id");

    // set unsubscribe
    if (sessionManager.unSubscribeChannel(session, unsub)) {
      session.writeText(Json.encode(Response.unSubOK(id, unsub)));
    } else {
      session.writeText(Json.encode(Response.err(id, unsub, "invalid channel of: " + unsub + " server not support!")));
    }
    return true;
  }

  @Override
  public void onDepthUpdate(String symbol, DepthLevel level, DepthTick tick) {
    String marketDetailCh = ChannelUtil.buildMarketDepthChannel(symbol, level);
    TemplatePayload<DepthTick> detail = TemplatePayload.of(marketDetailCh, tick);
    sessionManager.foreachSessionByChannel(marketDetailCh, session -> session.writeText(Json.encode(detail)));
  }
}
