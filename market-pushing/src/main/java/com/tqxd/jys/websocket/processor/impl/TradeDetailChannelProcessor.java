package com.tqxd.jys.websocket.processor.impl;

import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTickData;
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
import java.util.List;
import java.util.Objects;

/**
 * 成交记录主题处理器
 *
 * @author lyqingye
 */
@SuppressWarnings("Duplicates")
public class TradeDetailChannelProcessor implements ChannelProcessor {
  private CacheManager cacheManager;
  private SessionManager sessionManager;

  public TradeDetailChannelProcessor (CacheManager cacheManager,SessionManager sessionManager) {
    this.cacheManager = Objects.requireNonNull(cacheManager);
    this.sessionManager = Objects.requireNonNull(sessionManager);
  }

  @Override
  public boolean doReqIfChannelMatched(String ch, Session session, JsonObject json) {
    if (!ChannelUtil.isTradeDetailChannel(ch)) {
      return false;
    }
    String id = json.getString("id");
    List<TradeDetailTickData> tickList = cacheManager.reqTradeDetail(ChannelUtil.getSymbol(ch),30);
    TradeDetailTick resp = new TradeDetailTick();
    resp.setData(tickList);
    session.writeBufferAndCompress(Json.encodeToBuffer(Response.reqOk(id, ch, resp)));
    return true;
  }

  @Override
  public boolean doSubIfChannelMatched(String sub, Session session, JsonObject json) {
    if (!ChannelUtil.isTradeDetailChannel(sub)) {
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
    if (!ChannelUtil.isTradeDetailChannel(unsub)) {
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
  public void onTradeDetailUpdate(String symbol, TradeDetailTick tick) {
    String tradeDetailCh = ChannelUtil.buildTradeDetailChannel(symbol);
    TemplatePayload<TradeDetailTick> detail = TemplatePayload.of(tradeDetailCh, tick);

    try {
      Buffer buffer = Buffer.buffer(GZIPUtils.fastCompress(Json.encodeToBuffer(detail).getBytes()));
      sessionManager.foreachSessionByChannel(tradeDetailCh, session -> session.writeBuffer(buffer));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
