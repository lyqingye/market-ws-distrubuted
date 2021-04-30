package com.tqxd.jys.websocket;

import com.tqxd.jys.websocket.session.FastSessionMgr;
import com.tqxd.jys.websocket.session.Session;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 应用模块名称:
 * 代码描述:
 * Copyright: Copyright (C) 2021, Inc. All rights reserved.
 * Company:
 *
 * @author
 * @since 2021/4/24 9:50
 */
public class ServerEndpoint extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(ServerEndpoint.class);

    /**
     * websocket 服务器
     */
    private HttpServer wsServer;

    /**
     * 会话管理器
     */
    private FastSessionMgr sessionMgr = new FastSessionMgr(1 << 16);
    private long expire = 10;
    private TimeUnit timeUnit = TimeUnit.SECONDS;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        wsServer = vertx.createHttpServer().webSocketHandler(client -> {
            Session newSession = sessionMgr.allocate();
            newSession.initSession(client, expire, timeUnit);
            client.frameHandler(frame -> {
                sessionMgr.refreshTTL(client, expire, timeUnit);
                if (frame.isText() && frame.isFinal()) {
                    System.out.println(frame.textData());
                } else {
                    log.warn("[KlineWorker]: binary frame is not supported!");
                }
            });

            client.exceptionHandler(throwable -> {
                if (sessionMgr.release(newSession)) {
                    log.warn("release session fail! sessionId: {}", newSession.id());
                }
                // 清理会话
                throwable.printStackTrace();
            });

            client.closeHandler(ignored -> {
                if (sessionMgr.release(newSession)) {
                    log.warn("release session fail! sessionId: {}", newSession.id());
                }
                // 清理会话
                log.info("{} close", client.textHandlerID());
            });
        });

        wsServer.listen(7776,"localhost")
                .onComplete(h -> {
                    if (h.succeeded()){
                        log.info("[ServerEndpoint]: start success!");
                        startPromise.complete();
                    }else {
                        startPromise.fail(h.cause());
                    }
                });
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        if (wsServer != null) {
            log.info("[ServerEndpoint]: stop the websocket server!");
            wsServer.close(stopPromise);
        }
    }
}
