package com.tqxd.jys.websocket.processor.impl;

import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.websocket.processor.ChannelProcessor;
import com.tqxd.jys.websocket.processor.Context;
import com.tqxd.jys.websocket.processor.Response;
import com.tqxd.jys.websocket.session.Session;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

/**
 * k线主题处理器
 */
public class KLineChannelProcessor implements ChannelProcessor {
  private static final String KLINE_TOPIC = "kline";

  @Override
  public boolean doReqIfChannelMatched(Context ctx, String ch, Session session, JsonObject json) {
    if (isTopicNotMatch(ch)) {
      return false;
    }
    String id = json.getString("id");
    String req = json.getString("req");
    Long from = json.getLong("from");
    Long to = json.getLong("to");
    ChannelUtil.KLineChannel channel = ChannelUtil.resolveKLineCh(ch);
    if (channel == null) {
      session.writeText(Json.encode(Response.err(id, req, "invalid kline channel string! format: market.$symbol.kline.$period, example: market.ethbtc.kline.1min")));
      return true;
    }
    if (from == null || to == null) {
      session.writeText(Json.encode(Response.err(id, req,
          "invalid kline args format: {\n" +
              "  \"req\": \"market.ethbtc.kline.1min\",\n" +
              "  \"id\": \"client generate\",\n" +
              "  \"from\": \"start unix ts\",\n" +
              "  \"to\": \"end unix ts\"\n" +
              "}")));
      return true;
    }
    // unix 时间戳转换是为了适配火币
    ctx.getKlineManager().pollTicks(channel.getSymbol(), channel.getPeriod(), from * 1000, to * 1000, h -> {
      if (h.succeeded()) {
        session.writeText(Json.encode(Response.kLineReqOk(id, req, h.result())));
      } else {
        session.writeText(Json.encode(Response.err(id, req, "server internal error!")));
        h.cause().printStackTrace();
      }
    });
    return true;
  }

  @Override
  public boolean doSubIfChannelMatched(Context ctx, String ch, Session session, JsonObject json) {
    if (isTopicNotMatch(ch)) {
      return false;
    }
    String id = json.getString("id");
    String req = json.getString("req");
    ChannelUtil.KLineChannel channel = ChannelUtil.resolveKLineCh(ch);
    if (channel == null) {
      session.writeText(Json.encode(Response.err(id, req, "invalid kline channel string! format: market.$symbol.kline.$period, example: market.ethbtc.kline.1min")));
      return true;
    }
    // set subscribe
    session.setAttr(ch, System.currentTimeMillis());
    return true;
  }

  @Override
  public boolean doUnSubIfChannelMatched(Context ctx, String ch, Session session, JsonObject json) {
    if (isTopicNotMatch(ch)) {
      return false;
    }
    String id = json.getString("id");
    String req = json.getString("req");
    ChannelUtil.KLineChannel channel = ChannelUtil.resolveKLineCh(ch);
    if (channel == null) {
      session.writeText(Json.encode(Response.err(id, req, "invalid kline channel string! format: market.$symbol.kline.$period, example: market.ethbtc.kline.1min")));
      return true;
    }
    // set subscribe
    session.setAttr(ch, null);
    return false;
  }

  private boolean isTopicNotMatch(String ch) {
    return !KLINE_TOPIC.equalsIgnoreCase(ChannelUtil.getTopic(ch));
  }
}
