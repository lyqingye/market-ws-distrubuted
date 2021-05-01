package com.tqxd.jys.websocket;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.timeline.KLineManager;
import com.tqxd.jys.timeline.KLineMeta;
import com.tqxd.jys.timeline.cmd.ApplyTickResult;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.websocket.processor.ChannelProcessor;
import com.tqxd.jys.websocket.processor.Context;
import com.tqxd.jys.websocket.processor.Response;
import com.tqxd.jys.websocket.processor.impl.KLineChannelProcessor;
import com.tqxd.jys.websocket.session.FastSessionMgr;
import com.tqxd.jys.websocket.session.Session;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
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

    private static final ChannelProcessor PROCESSORS[] = {new KLineChannelProcessor()};

    /**
     * 会话管理器
     */
    private FastSessionMgr sessionMgr = new FastSessionMgr(1 << 16);
    private Context context;
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    private long expire = 30;

    public ServerEndpointVerticle(KLineManager kLineManager) {
        kLineManager.setOutResultConsumer(this::onUpdateData);
        context = new Context(sessionMgr, kLineManager);
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
        wsServer = vertx.createHttpServer().webSocketHandler(client -> {
            Session session = sessionMgr.allocate();
            session.initSession(client, expire, timeUnit);
            client.frameHandler(frame -> {
                sessionMgr.refreshTTL(client, expire, timeUnit);
                if (frame.isText() && frame.isFinal()) {
                    this.onReceiveTextMsg(session, frame.textData());
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

        wsServer.listen(7776,"localhost")
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
     * 处理消息
     *
     * @param session 会话
     * @param msg     消息
     */
    private void onReceiveTextMsg(Session session, String msg) {
        JsonObject jsonObj;
        try{
         jsonObj = (JsonObject) Json.decodeValue(msg);
        }catch (Exception ex) {
          session.writeText(Json.encode(Response.err(null,null,"only support json message!")));
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
            for (ChannelProcessor processor : PROCESSORS) {
                if (processor.doReqIfChannelMatched(context, req, session, jsonObj)) {
                    return;
                }
            }
        } else if (sub != null) {
            for (ChannelProcessor processor : PROCESSORS) {
                if (processor.doSubIfChannelMatched(context, sub, session, jsonObj)) {
                    return;
                }
            }
        } else if (unsub != null) {
            for (ChannelProcessor processor : PROCESSORS) {
                if (processor.doUnSubIfChannelMatched(context, unsub, session, jsonObj)) {
                    return;
                }
            }
        } else {
            session.writeText(Json.encode(Response.err(null,null,"unknown message: {}" + msg)));
            log.warn("[ServerEndpoint]: unknown message: {}", msg);
        }
    }

    private void onUpdateData(Object data) {
        if (data instanceof ApplyTickResult) {
            ApplyTickResult result = (ApplyTickResult) data;
            KLineMeta meta = result.getMeta();
            String kLineTickCh = ChannelUtil.buildKLineTickChannel(meta.getSymbol(), meta.getPeriod());
            TemplatePayload<KlineTick> tick = TemplatePayload.of(kLineTickCh,result.getTick());
            sessionMgr.foreachSessionByChannel(kLineTickCh,session -> {
                session.writeText(Json.encode(tick));
            });
        } else if (data instanceof TemplatePayload) {
            System.out.println();
        }
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
