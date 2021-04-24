package com.tqxd.jys.websocket;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        wsServer = vertx.createHttpServer().webSocketHandler(chanel -> {
            chanel.frameHandler(frame -> {
                if (frame.isText() && frame.isFinal()) {
                    System.out.println(frame.textData());
                }else {
                    log.warn("[KlineWorker]: binary frame is not supported!");
                }
            });

            chanel.endHandler(h -> {
                log.info("{} end", chanel.textHandlerID());
            });

            chanel.exceptionHandler(throwable -> {
                // 清理会话
                throwable.printStackTrace();
            });
            chanel.closeHandler(ignored -> {
                // 清理会话
                log.info("{} close", chanel.textHandlerID());
            });
        });

        wsServer.listen(7776,"localhost")
                .onComplete(h -> {
                    if (h.succeeded()){
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
