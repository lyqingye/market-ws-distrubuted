package com.tqxd.jys.collectors.api;

import com.tqxd.jys.collectors.impl.Collector;
import com.tqxd.jys.collectors.impl.HuoBiKlineCollector;
import com.tqxd.jys.constance.CollectDataType;
import com.tqxd.jys.messagebus.MessageBus;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.servicebus.payload.CollectorStatusDto;
import com.tqxd.jys.servicebus.service.collectors.CollectorOpenApi;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 收集器服务
 */
public class CollectorOpenApiImpl implements CollectorOpenApi {
    private static final Logger log = LoggerFactory.getLogger(CollectorOpenApiImpl.class);

    /**
     * vertx 实例
     */
    private Vertx vertx;

    /**
     * msg bus
     */
    private MessageBus msgBus;

    public CollectorOpenApiImpl(Vertx vertx, MessageBus msgBus) {
        this.vertx = Objects.requireNonNull(vertx);
        this.msgBus = Objects.requireNonNull(msgBus);

        // 注册支持的收集器
        HuoBiKlineCollector huoBi = new HuoBiKlineCollector();
        collectorMap.put(huoBi.name(), huoBi);

        // 定时重启收集器免得被踢掉
        vertx.setPeriodic(TimeUnit.MINUTES.toMillis(10), timeId -> {
            deployMap.values().forEach(collector -> {
                if (collector.stop()) {
                    log.info("[KlineCollector]: stop collector: {} success!" ,collector.name());
                    collector.start(ar -> {
                        if (ar.succeeded()) {
                            log.info("[KlineCollector]: start collector: {} success!" ,collector.name());
                        } else {
                            ar.cause().printStackTrace();
                        }
                    });
                } else {
                    log.error("[KlineCollector]: stop collector: {}  fail!" , collector.name());
                }
            });
        });
    }

    /**
     * 收集器 map
     * <p>
     * 收集器名称 -> 收集器对象
     */
    private final Map<String, Collector> collectorMap = new ConcurrentHashMap<>(1);

    /**
     * 已经部署的收集器 map
     * <p>
     * 收集器名称 -> 收集器对象
     */
    private final Map<String, Collector> deployMap = new ConcurrentHashMap<>(1);


    /**
     * 获取所有收集器状态
     *
     * @param handler 处理器
     */
    @Override
    public void listCollector(Handler<AsyncResult<List<CollectorStatusDto>>> handler) {
        List<CollectorStatusDto> result = collectorMap.values()
                .stream()
                .map(Collector::snapStatus)
                .collect(Collectors.toList());
        handler.handle(Future.succeededFuture(result));
    }

    /**
     * 部署一个收集器
     *
     * @param collectorName 收集器名称
     * @param configJson    收集器配置
     * @return 是否部署成功
     */
    @Override
    public void deployCollectorEx(String collectorName, String configJson,
                                  Handler<AsyncResult<Boolean>> handler) {
        Collector collector = collectorMap.get(collectorName);
        if (collector == null) {
            handler.handle(Future.failedFuture("collector not found"));
            return;
        }
        if (deployMap.containsKey(collectorName)) {
            handler.handle(Future.succeededFuture(true));
            return;
        }
        JsonObject config = null;
        if (configJson != null) {
            try {
                config = (JsonObject) Json.decodeValue(configJson);
            } catch (DecodeException ex) {
                handler.handle(Future.failedFuture(ex));
            }
        }
        if (collector.deploy(vertx,
                (type, data) -> {
                    Topic topic;
                    switch (type) {
                        case KLINE: {
                            topic = Topic.KLINE_TICK_TOPIC;
                            break;
                        }
                        default:
                            throw new IllegalStateException("Unexpected value: " + type);
                    }
                    // 异步数据处理
                    VertxUtil.asyncFastCallIgnoreRs(vertx, () -> {
                        // 推送k线数据
                        msgBus.publishIgnoreRs(topic, Message.withData(collectorName, data.encode()));
                    });
                }, config)) {
            deployMap.put(collectorName, collector);
            handler.handle(Future.succeededFuture(true));
        } else {
            handler.handle(Future.failedFuture("deploy fail"));
        }

    }

    /**
     * 部署一个收集器
     *
     * @param collectorName 收集器名称
     * @return 是否部署成功
     */
    @Override
    public void deployCollector(String collectorName,
                                Handler<AsyncResult<Boolean>> handler) {
        deployCollectorEx(collectorName, null, handler);
    }

    /**
     * 取消部署一个收集器
     *
     * @param collectorName 收集器名称
     * @return 是否取消部署成功
     */
    @Override
    public void unDeployCollector(String collectorName,
                                  Handler<AsyncResult<Boolean>> handler) {
        Collector collector = deployMap.get(collectorName);
        if (collector == null) {
            handler.handle(Future.failedFuture("collector not found"));
            return;
        }
        if (collector.unDeploy(null)) {
            deployMap.remove(collectorName);
            handler.handle(Future.succeededFuture(true));
        } else {
            handler.handle(Future.failedFuture("unDeploy fail"));
        }
    }

    /**
     * 启动收集器
     *
     * @param collectorName 收集器名称
     * @return 启动结果
     */
    @Override
    public void startCollector(String collectorName,
                               Handler<AsyncResult<Boolean>> handler) {
        Collector collector = deployMap.get(collectorName);
        if (collector == null) {
            handler.handle(Future.failedFuture("collector not found"));
            return;
        }
        collector.start(handler);
    }

    /**
     * 停止收集器
     *
     * @param collectorName 收集器名称
     * @return 是否停止成功
     */
    @Override
    public void stopCollector(String collectorName,
                              Handler<AsyncResult<Boolean>> handler) {
        Collector collector = deployMap.get(collectorName);
        if (collector == null) {
            handler.handle(Future.failedFuture("collector not found"));
            return;
        }
        if (collector.stop()) {
            handler.handle(Future.succeededFuture(true));
        } else {
            handler.handle(Future.failedFuture("fail to stop"));
        }
    }

    /**
     * 订阅交易对
     *
     * @param collectDataType 收集的数据类型
     * @param collectorName   收集器名称
     * @param symbol          交易对
     * @return 是否订阅成功
     */
    @Override
    public void subscribe(String collectorName, CollectDataType collectDataType, String symbol,
                          Handler<AsyncResult<Boolean>> handler) {
        Collector collector = deployMap.get(collectorName);
        if (collector == null) {
            handler.handle(Future.failedFuture("collector not found"));
            return;
        }
        if (collector.subscribe(collectDataType, symbol)) {
            handler.handle(Future.succeededFuture(true));
        } else {
            handler.handle(Future.failedFuture("subscribe fail"));
        }
    }

    /**
     * 取消订阅交易对
     *
     * @param collectDataType 收集的数据类型
     * @param collectorName   收集器名称
     * @param symbol          交易对
     * @return 是否取消成功
     */
    @Override
    public void unsubscribe(String collectorName, CollectDataType collectDataType, String symbol,
                            Handler<AsyncResult<Boolean>> handler) {
        Collector collector = deployMap.get(collectorName);
        if (collector == null) {
            handler.handle(Future.failedFuture("collector not found"));
            return;
        }
        if (collector.unSubscribe(collectDataType, symbol)) {
            handler.handle(Future.succeededFuture(true));
        } else {
            handler.handle(Future.failedFuture("unSubscribe fail"));
        }
    }


    //
    // future api
    //

    /**
     * 部署一个收集器
     *
     * @param collectorName 收集器名称
     * @return future
     */
    public Future<Boolean> deployCollector(String collectorName) {
        Promise<Boolean> promise = Promise.promise();
        deployCollector(collectorName,promise);
        return promise.future();
    }

    /**
     * 启动收集器
     *
     * @param collectorName 收集器名称
     * @return future
     */
    public Future<Boolean> startCollector(String collectorName) {
        Promise<Boolean> promise = Promise.promise();
        startCollector(collectorName,promise);
        return promise.future();
    }

    /**
     * 订阅交易对
     *
     * @param collectDataType 收集的数据类型
     * @param collectorName   收集器名称
     * @param symbol          交易对
     * @return future
     */
    public Future<Boolean> subscribe(String collectorName, CollectDataType collectDataType, String symbol) {
        Promise<Boolean> promise = Promise.promise();
        subscribe(collectorName, collectDataType, symbol, promise);
        return promise.future();
    }
}
