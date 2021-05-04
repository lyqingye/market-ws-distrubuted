package com.tqxd.jys.websocket.transport;

import com.tqxd.jys.websocket.processor.ChannelProcessor;
import com.tqxd.jys.websocket.session.Session;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 请求分发器
 *
 * @author lyqingye
 */
public class RequestDispatcher {
  private static final Logger log = LoggerFactory.getLogger(RequestDispatcher.class);
  private int numOfProcessor = 0;
  private ChannelProcessor[] PROCESSORS = new ChannelProcessor[255];

  public synchronized void addProcessor (ChannelProcessor processor) {
    if (numOfProcessor >= PROCESSORS.length) {
      ChannelProcessor[] newProcessor = new ChannelProcessor[numOfProcessor << 1];
      System.arraycopy(PROCESSORS,0,newProcessor,0,numOfProcessor);
      PROCESSORS = newProcessor;
    }
    PROCESSORS[numOfProcessor++] = Objects.requireNonNull(processor);
  }

  /**
   * 处理消息
   *
   * @param session 会话
   * @param msg     消息
   */
  public void onReceiveTextMsg(Session session, String msg) {
    JsonObject jsonObj;
    try {
      jsonObj = (JsonObject) Json.decodeValue(msg);
    } catch (Exception ex) {
      session.writeText(Json.encode(Response.err(null, null, "only support json message!")));
      return;
    }
    // 忽略心跳
    if (jsonObj.containsKey("ping")) {
      return;
    }
    String req = jsonObj.getString("req");
    String sub = jsonObj.getString("sub");
    String unsub = jsonObj.getString("unsub");
    if (req != null) {
      for (int i = 0; i < numOfProcessor; i++) {
        if (PROCESSORS[i].doReqIfChannelMatched(req, session, jsonObj)) {
          return;
        }
      }

    } else if (sub != null) {
      for (int i = 0; i < numOfProcessor; i++) {
        if (PROCESSORS[i].doSubIfChannelMatched(sub, session, jsonObj)) {
          return;
        }
      }
    } else if (unsub != null) {
      for (int i = 0; i < numOfProcessor; i++) {
        if (PROCESSORS[i].doUnSubIfChannelMatched(unsub, session, jsonObj)) {
          return;
        }
      }
    } else {
      session.writeText(Json.encode(Response.err(null, null, "unknown message: {}" + msg)));
      log.warn("[ServerEndpoint]: unknown message: {}", msg);
    }
  }
}
