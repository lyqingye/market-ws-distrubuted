package com.tqxd.jys.websocket.transport;

import com.tqxd.jys.utils.VertxUtil;
import com.tqxd.jys.websocket.session.Session;
import com.tqxd.jys.websocket.session.SessionManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.VertxException;
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
  private static final String WEBSOCKET_HOST_CONFIG = "market.pushing.websocket.host";
  private static final String WEBSOCKET_PORT_CONFIG = "market.pushing.websocket.port";
  private static final String WEBSOCKET_PATH_CONFIG = "market.pushing.websocket.path";
  private static final Logger log = LoggerFactory.getLogger(ServerEndpointVerticle.class);

  /**
   * websocket 服务器
   */
  private HttpServer wsServer;
  /**
   * 会话管理器
   */
  private SessionManager sessionMgr;
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
    sessionMgr = SessionManager.getInstance(config());
    HttpServerOptions options = new HttpServerOptions();
    options.setTcpNoDelay(true);
    options.setSendBufferSize(4096);
    String host = VertxUtil.jsonGetValue(config(), WEBSOCKET_HOST_CONFIG, String.class, "0.0.0.0");
    Integer port = VertxUtil.jsonGetValue(config(), WEBSOCKET_PORT_CONFIG, Integer.class, 7776);
    String path = VertxUtil.jsonGetValue(config(), WEBSOCKET_PATH_CONFIG, String.class, "/");
    wsServer = vertx.createHttpServer(options).webSocketHandler(client -> {
      if (!path.equals(client.path())) {
        client.reject();
        return;
      }
      Session session = sessionMgr.allocate();
      if (session == null) {
        client.reject();
        log.error("[KlineWorker]: allocate session fail! ");
        return;
      }
      // 将vertx会话绑定到我们封装的会话中
      session.bindVertxNativeSession(client, expire, timeUnit);
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
        // 这种异常会由close handler处理，所以不需要重复处理
        if (!(throwable instanceof VertxException) || !"Connection was closed".equals(throwable.getMessage())) {
          safeRelease(session);
        }
        throwable.printStackTrace();
      });
      client.closeHandler(ignored -> safeRelease(session));
    });

    wsServer.listen(port, host)
        .onComplete(h -> {
          if (h.succeeded()) {
            log.info("[ServerEndpoint]: start success! listen on: ws://{}:{}{}", host, port, path);
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
