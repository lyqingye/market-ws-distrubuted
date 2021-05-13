package com.tqxd.jys.websocket.processor.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.utils.GZIPUtils;
import com.tqxd.jys.websocket.cache.CacheManager;
import com.tqxd.jys.websocket.processor.ChannelProcessor;
import com.tqxd.jys.websocket.session.Session;
import com.tqxd.jys.websocket.session.SessionManager;
import com.tqxd.jys.websocket.transport.Response;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.Objects;

/**
 * 市场概括主题处理器
 *
 * @author lyqingye
 */
@SuppressWarnings("Duplicates")
public class MarketDetailChannelProcessor implements ChannelProcessor {
  private CacheManager cacheManager;
  private SessionManager sessionManager;

  public MarketDetailChannelProcessor (CacheManager cacheManager,SessionManager sessionManager) {
    this.cacheManager = Objects.requireNonNull(cacheManager);
    this.sessionManager = Objects.requireNonNull(sessionManager);
  }

  @Override
  public boolean doReqIfChannelMatched(String ch, Session session, JsonObject json) {
    if (!ChannelUtil.isMarketDetailChannel(ch)) {
      return false;
    }
    String id = json.getString("id");
    KlineTick tick = cacheManager.reqMarketDetail(ChannelUtil.getSymbol(ch));
    session.writeBufferAndCompress(Json.encodeToBuffer(Response.reqOk(id, ch, tick)));
    return true;
  }

  @Override
  public boolean doSubIfChannelMatched(String sub, Session session, JsonObject json) {
    if (!ChannelUtil.isMarketDetailChannel(sub)) {
      return false;
    }
    String id = json.getString("id");

    // set subscribe
    if (sessionManager.subscribeChannel(session,sub)) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.subOK(id, sub)));
    }else{
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, sub, "invalid channel of: " + sub + " server not support!")));
    }
    return true;
  }

  @Override
  public boolean doUnSubIfChannelMatched(String unsub, Session session, JsonObject json) {
    if (!ChannelUtil.isMarketDetailChannel(unsub)) {
      return false;
    }
    String id = json.getString("id");

    // set unsubscribe
    if (sessionManager.unSubscribeChannel(session, unsub)) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.unSubOK(id, unsub)));
    } else {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, unsub, "invalid channel of: " + unsub + " server not support!")));
    }
    return true;
  }

  @Override
  public void onMarketDetailUpdate(String symbol, KlineTick tick) {
    String marketDetailCh = ChannelUtil.buildMarketDetailChannel(symbol);
    TemplatePayload<KlineTick> detail = TemplatePayload.of(marketDetailCh, tick);

    try {
      Buffer buffer = Buffer.buffer(GZIPUtils.fastCompress(Json.encodeToBuffer(detail).getBytes()));
      sessionManager.foreachSessionByChannel(marketDetailCh, session -> session.writeBuffer(buffer));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
