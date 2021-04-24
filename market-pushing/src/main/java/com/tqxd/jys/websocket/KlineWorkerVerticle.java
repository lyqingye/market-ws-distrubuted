package com.tqxd.jys.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.openapi.RepositoryOpenApi;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.timeline.KlineTimeLine;
import com.tqxd.jys.timeline.KlineTimeManager;
import com.tqxd.jys.timeline.cmd.CmdResult;
import com.tqxd.jys.timeline.cmd.UpdateTickResult;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.JacksonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 应用模块名称:
 * 代码描述:
 * Copyright: Copyright (C) 2021, Inc. All rights reserved.
 * Company:
 *
 * @author
 * @since 2021/4/23 21:40
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class KlineWorkerVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(KlineWorkerVerticle.class);

    /**
     * k线管理器
     */
    private KlineTimeManager klineManager;

    /**
     * 持久化仓库 open api
     */
    private RepositoryOpenApi repository;


    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        repository = RepositoryOpenApi.createProxy(vertx);
        long startTime = System.currentTimeMillis();
        CompositeFuture.join(initKline(),listenKlineMessageTopic())
                .onFailure(startPromise::fail)
                .onSuccess(h -> {
                    log.info("[KlineWorker]: start kline worker success! using {}ms",System.currentTimeMillis() - startTime);
                });
    }

    private Future initKline () {
        // 初始化k线管理器
        klineManager = KlineTimeManager.create(vertx,(meta,detail) -> {
           // TODO 窗口滑动，生成市场详情数据, 需要做处理
        });
        // 初始化k线快照信息
        return listKlineKeys().compose(keys -> {
            if (keys.isEmpty()) {
                return Future.succeededFuture();
            }
            List<Future> allKlineFutures = new ArrayList<>(keys.size());
            for (String klineKey : keys) {
                final long startTime = System.currentTimeMillis();
                Future future = getKlineSnapshot(klineKey)
                        .compose(snapshot -> {
                            for (Period period : Period.values()) {
                                KlineTimeLine timeLine = klineManager.getOrCreate(klineKey, period);
                                CmdResult<MarketDetailTick> applyRs = timeLine.applySnapshot(snapshot.getCommittedIndex(), snapshot.getTickList());
                                if (Period._1_MIN.equals(period)) {
                                    try {
                                        MarketDetailTick detail = applyRs.get();
                                        // TODO　市场详情数据处理
                                        System.out.println();
                                    } catch (Exception ex) {
                                        ex.printStackTrace();
                                    }
                                }
                            }
                            log.info("[KlineWorker]: init kline: {} size: {} using: {}ms", klineKey, snapshot.getTickList().size(), System.currentTimeMillis() - startTime);
                            return Future.succeededFuture();
                        });
                allKlineFutures.add(future);
            }
            return CompositeFuture.all(allKlineFutures);
        });
    }

    private Future<Set<String>> listKlineKeys () {
        Promise<Set<String>> promise = Promise.promise();
        repository.listKlineKeys(promise);
        return promise.future();
    }

    private Future<KlineSnapshot> getKlineSnapshot (String klineKey) {
        Promise<KlineSnapshot> promise = Promise.promise();
        repository.getKlineSnapshot(klineKey,h -> {
            if (h.succeeded()) {
                promise.complete(Json.decodeValue(h.result(),KlineSnapshot.class));
            }else {
                promise.fail(h.cause());
            }
        });
        return promise.future();
    }

    private Future listenKlineMessageTopic () {
        Promise promise = Promise.promise();
        MessageBusFactory.bus().subscribe(Topic.KLINE_TICK_TOPIC,this::processKlineMsg, promise);
        return promise.future();
    }

    private void processKlineMsg (Message<?> msg) {
//        switch (msg.getType()) {
//            case KLINE: {
//                TemplatePayload<KlineTick> payload = JacksonCodec.decodeValue((String) msg.getPayload(), new TypeReference<TemplatePayload<KlineTick>>() {
//                });
//                log.info("[KlineWorker]: apply msgIndex: {}, payload: {}", msg.getIndex(), msg.getPayload());
//                CmdResult<UpdateTickResult> updateResult = klineManager.updateKline(payload.getCh(), Period._1_MIN, commitIndex, tick);
//                UpdateTickResult updatedResult = null;
//                try {
//                    updatedResult = updateResult.get();
//                } catch (InterruptedException | ExecutionException e) {
//                    e.printStackTrace();
//                    return;
//                }
//                if (updateResult.isSuccess()) {
//
//                } else {
////                    log.warn("[Kline-Repository]: update kline tick fail! reason: {}, commitIndex: {} payload: {}", updateResult.getReason(), commitIndex, Json.encode(payload));
//                }
//                break;
//            }
//            default:
//                log.error("[KlineWorker]: invalid message type from Kline topic! message: {}", msg);
//        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        super.stop(stopPromise);
    }
}
