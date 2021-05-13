package com.tqxd.jys.websocket.processor.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.Period;
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
 * k线主题处理器
 */
@SuppressWarnings("Duplicates")
public class KLineChannelProcessor implements ChannelProcessor {
  private static final String KLINE_TOPIC = "kline";
  private SessionManager sessionManager;
  private CacheManager cacheManager;

  public KLineChannelProcessor (CacheManager cacheManager,SessionManager sessionManager) {
    this.sessionManager = Objects.requireNonNull(sessionManager);
    this.cacheManager = Objects.requireNonNull(cacheManager);
  }

  @Override
  public boolean doReqIfChannelMatched(String req, Session session, JsonObject json) {
    if (isTopicNotMatch(req)) {
      return false;
    }
    String id = json.getString("id");
    Long from = json.getLong("from");
    Long to = json.getLong("to");
    ChannelUtil.KLineChannel channel = ChannelUtil.resolveKLineCh(req);
    if (channel == null) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, req, "invalid kline channel string! format: market.$symbol.kline.$period, example: market.ethbtc.kline.1min")));
      return true;
    }
    if (from == null || to == null) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, req,
          "invalid kline args format: {\n" +
              "  \"req\": \"market.ethbtc.kline.1min\",\n" +
              "  \"id\": \"client generate\",\n" +
              "  \"from\": \"start unix ts\",\n" +
              "  \"to\": \"end unix ts\"\n" +
              "}")));
      return true;
    }
    // unix 时间戳转换是为了适配火币
     cacheManager.reqKlineHistory(channel.getSymbol(), channel.getPeriod(), from, to, h -> {
       if (h.succeeded()) {
         session.writeBufferAndCompress(Json.encodeToBuffer(Response.reqOk(id, req, h.result())));
       } else {
         session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, req, "server internal error!")));
         h.cause().printStackTrace();
       }
     });
    return true;
  }

  @Override
  public boolean doSubIfChannelMatched(String sub, Session session, JsonObject json) {
    if (isTopicNotMatch(sub)) {
      return false;
    }
    String id = json.getString("id");
    ChannelUtil.KLineChannel channel = ChannelUtil.resolveKLineCh(sub);
    if (channel == null) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, sub, "invalid kline channel string! format: market.$symbol.kline.$period, example: market.ethbtc.kline.1min")));
      return true;
    }
    // set subscribe
    if (sessionManager.subscribeChannel(session,sub)) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.subOK(id, sub)));
    }else{
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, sub, "invalid channel of: " + sub + " server not support!")));
    }
    return true;
  }

  @Override
  public boolean doUnSubIfChannelMatched(String ch, Session session, JsonObject json) {
    if (isTopicNotMatch(ch)) {
      return false;
    }
    String id = json.getString("id");
    ChannelUtil.KLineChannel channel = ChannelUtil.resolveKLineCh(ch);
    if (channel == null) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, ch, "invalid kline channel string! format: market.$symbol.kline.$period, example: market.ethbtc.kline.1min")));
      return true;
    }
    // set subscribe
    if (sessionManager.unSubscribeChannel(session, ch)) {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.unSubOK(id, ch)));
    } else {
      session.writeBufferAndCompress(Json.encodeToBuffer(Response.err(id, ch, "server internal error!")));
    }
    return false;
  }

  @Override
  public void onKLineUpdate(String symbol, Period period, KlineTick tick) {
    // 广播消息
    String kLineTickCh = ChannelUtil.buildKLineTickChannel(symbol, period);
    TemplatePayload<KlineTick> data = TemplatePayload.of(kLineTickCh, tick);

    try {
      Buffer buffer = Buffer.buffer(GZIPUtils.fastCompress(Json.encodeToBuffer(data).getBytes()));
      sessionManager.foreachSessionByChannel(kLineTickCh, session -> session.writeBuffer(buffer));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private boolean isTopicNotMatch(String ch) {
    return !KLINE_TOPIC.equalsIgnoreCase(ChannelUtil.getTopic(ch));
  }
}
