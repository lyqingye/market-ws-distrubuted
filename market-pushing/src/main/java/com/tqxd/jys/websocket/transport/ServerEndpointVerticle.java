package com.tqxd.jys.websocket.transport;

import com.tqxd.jys.websocket.cache.CacheManager;
import com.tqxd.jys.websocket.processor.impl.KLineChannelProcessor;
import com.tqxd.jys.websocket.processor.impl.MarketDepthChannelProcessor;
import com.tqxd.jys.websocket.processor.impl.MarketDetailChannelProcessor;
import com.tqxd.jys.websocket.processor.impl.TradeDetailChannelProcessor;
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
  private SessionManager sessionMgr = new SessionManager(1 << 14);
  private TimeUnit timeUnit = TimeUnit.SECONDS;
  private long expire = -1;
  private RequestDispatcher dispatcher;

  public ServerEndpointVerticle(CacheManager cacheManager) {
    dispatcher = new RequestDispatcher();
    // k线主题处理器
    KLineChannelProcessor kLineChannelProcessor = new KLineChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addListener(kLineChannelProcessor);
    dispatcher.addProcessor(kLineChannelProcessor);

    // 成交记录主题处理器
    TradeDetailChannelProcessor tradeDetailChannelProcessor = new TradeDetailChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addListener(tradeDetailChannelProcessor);
    dispatcher.addProcessor(tradeDetailChannelProcessor);

    // 市场概括主题处理器
    MarketDetailChannelProcessor marketDetailChannelProcessor = new MarketDetailChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addListener(marketDetailChannelProcessor);
    dispatcher.addProcessor(marketDetailChannelProcessor);

    // 市场深度主题处理器
    MarketDepthChannelProcessor marketDepthChannelProcessor = new MarketDepthChannelProcessor(cacheManager, sessionMgr);
    cacheManager.addListener(marketDepthChannelProcessor);
    dispatcher.addProcessor(marketDepthChannelProcessor);
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

    wsServer.listen(7776, "localhost")
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
