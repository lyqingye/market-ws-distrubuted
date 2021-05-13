package com.tqxd.jys.websocket.transport;

import com.tqxd.jys.websocket.session.Session;
import com.tqxd.jys.websocket.session.SessionManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 消息推送服务
 *
 * @author lyqingye
 */
public class ServerEndpointVerticle extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(ServerEndpointVerticle.class);

  /**
   * websocket 服务器
   */
  private HttpServer wsServer;
  /**
   * 会话管理器
   */
  private SessionManager sessionMgr = SessionManager.getInstance();
  private TimeUnit timeUnit = TimeUnit.SECONDS;
  private long expire = -1;
  private RequestDispatcher dispatcher;

  public ServerEndpointVerticle() {
    dispatcher = RequestDispatcher.getInstance();
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    if (wsServer != null) {
      log.info("[ServerEndpoint]: stop the websocket server!");
      wsServer.close(stopPromise);
    }
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    HttpServerOptions options = new HttpServerOptions();
    options.setTcpNoDelay(true);
    options.setSendBufferSize(4096);
    wsServer = vertx.createHttpServer(options).webSocketHandler(client -> {
      Session session = sessionMgr.allocate();
      session.initSession(client, expire, timeUnit);
      client.frameHandler(frame -> {
        sessionMgr.refreshTTL(client, expire, timeUnit);
        if (frame.isText() && frame.isFinal()) {
          vertx.executeBlocking(prom -> {
            dispatcher.onReceiveTextMsg(session, frame.textData());
          }, false);
        } else {
          if (!frame.isClose()) {
            log.warn("[KlineWorker]: binary frame is not supported!");
          }
        }
      });
      client.exceptionHandler(throwable -> {
        safeRelease(session);
        throwable.printStackTrace();
      });
      client.closeHandler(ignored -> safeRelease(session));
    });

    wsServer.listen(7776, "0.0.0.0")
        .onComplete(h -> {
          if (h.succeeded()) {
            log.info("[ServerEndpoint]: start success!");
            startPromise.complete();
          } else {
            startPromise.fail(h.cause());
          }
        });
  }

  /**
   * 释放会话
   *
   * @param session 会话释放
   */
  private void safeRelease(Session session) {
    if (!sessionMgr.release(session)) {
      log.warn("release session fail! the session maybe already released! sessionId: {}", session.id());
    }
    sessionMgr.removeSessionSubscribedAllChannels(session);
  }
}
