package com.tqxd.jys.websocket.processor;

import com.tqxd.jys.websocket.session.Session;
import io.vertx.core.json.JsonObject;

/**
 * 主题处理器
 */
public interface ChannelProcessor {

  /**
   * 如果主题匹配那么直接处理Request请求
   *
   * @param ch      主题
   * @param session 会话
   * @param json    消息
   * @return 是否主题匹配
   */
  boolean doReqIfChannelMatched(Context ctx, String ch, Session session, JsonObject json);

  /**
   * 如果主题匹配那么直接处理Subscribe请求
   *
   * @param sub      主题
   * @param session 会话
   * @param json    消息
   * @return 是否主题匹配
   */
  boolean doSubIfChannelMatched(Context ctx, String sub, Session session, JsonObject json);

  /**
   * 如果主题匹配那么直接处理UnSubscribe请求
   *
   * @param unsub      主题
   * @param session 会话
   * @param json    消息
   * @return 是否主题匹配
   */
  boolean doUnSubIfChannelMatched(Context ctx, String unsub, Session session, JsonObject json);
}
