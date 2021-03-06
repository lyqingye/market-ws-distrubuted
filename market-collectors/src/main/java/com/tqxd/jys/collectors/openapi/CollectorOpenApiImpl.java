package com.tqxd.jys.collectors.openapi;

import com.tqxd.jys.collectors.impl.Collector;
import com.tqxd.jys.collectors.impl.DataReceiver;
import com.tqxd.jys.collectors.impl.binance.BiNanceCollector;
import com.tqxd.jys.collectors.impl.huobi.HuoBiKlineCollector;
import com.tqxd.jys.collectors.impl.tqxd.TqxdCollector;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.messagebus.MessageBus;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.openapi.CollectorOpenApi;
import com.tqxd.jys.openapi.payload.CollectorStatusDto;
import io.vertx.core.*;
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
public class CollectorOpenApiImpl implements CollectorOpenApi, DataReceiver {
  private static final Logger log = LoggerFactory.getLogger(CollectorOpenApiImpl.class);
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
   * vertx 实例
   */
  private Vertx vertx;
  /**
   * msg bus
   */
  private MessageBus msgBus;
  private Long lastTime;
  private int counter = 0;
  private long sizeOfByte = 0;

  public CollectorOpenApiImpl(Vertx vertx, MessageBus msgBus) {
    this.vertx = Objects.requireNonNull(vertx);
    this.msgBus = Objects.requireNonNull(msgBus);

    // 注册支持的收集器
    HuoBiKlineCollector huoBi = new HuoBiKlineCollector();
    collectorMap.put(huoBi.name(), huoBi);
    // 注册支持的收集器
    BiNanceCollector biNance = new BiNanceCollector();
    collectorMap.put(biNance.name(), biNance);
    // 注册天启旭达真实收集器
    TqxdCollector tqxd = new TqxdCollector();
    collectorMap.put(tqxd.name(), tqxd);
  }

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
  public void deployCollectorEx(String collectorName, JsonObject configJson,
                                Handler<AsyncResult<Void>> handler) {
    Collector collector = collectorMap.get(collectorName);
    if (collector == null) {
      handler.handle(Future.failedFuture("collector not found"));
      return;
    }
    if (deployMap.containsKey(collectorName)) {
      handler.handle(Future.succeededFuture());
      return;
    }
    vertx.deployVerticle(collector, new DeploymentOptions().setConfig(configJson))
      .onComplete(ar -> {
        if (ar.succeeded()) {
          deployMap.put(collectorName, collector);
          collector.addDataReceiver(this);
          handler.handle(Future.succeededFuture());
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
  }

  /**
   * 部署一个收集器
   *
   * @param collectorName 收集器名称
   * @return 是否部署成功
   */
  @Override
  public void deployCollector(String collectorName,
                              Handler<AsyncResult<Void>> handler) {
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
                                Handler<AsyncResult<Void>> handler) {
    Collector collector = deployMap.get(collectorName);
    if (collector == null) {
      handler.handle(Future.failedFuture("collector not found"));
    } else {
      vertx.undeploy(collector.deploymentID())
        .onComplete(ar -> {
          if (ar.succeeded()) {
            deployMap.remove(collectorName);
            handler.handle(Future.succeededFuture());
          } else {
            handler.handle(Future.failedFuture(ar.cause()));
          }
        });
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
                             Handler<AsyncResult<Void>> handler) {
    Collector collector = deployMap.get(collectorName);
    if (collector == null) {
      handler.handle(Future.failedFuture("collector not found"));
      return;
    }
    collector.startFuture().onComplete(handler);
  }


  //
  // future api
  //

  /**
   * 停止收集器
   *
   * @param collectorName 收集器名称
   * @return 是否停止成功
   */
  @Override
  public void stopCollector(String collectorName,
                            Handler<AsyncResult<Void>> handler) {
    Collector collector = deployMap.get(collectorName);
    if (collector == null) {
      handler.handle(Future.failedFuture("collector not found"));
      return;
    }
    collector.stopFuture().onComplete(handler);
  }

  /**
   * 订阅交易对
   *
   * @param dataType      收集的数据类型
   * @param collectorName 收集器名称
   * @param symbol        交易对
   * @return 是否订阅成功
   */
  @Override
  public void subscribe(String collectorName, DataType dataType, String symbol,
                        Handler<AsyncResult<Void>> handler) {
    Collector collector = deployMap.get(collectorName);
    if (collector == null) {
      handler.handle(Future.failedFuture("collector not found"));
      return;
    }
    collector.subscribe(dataType, symbol, handler);
  }

  /**
   * 取消订阅交易对
   *
   * @param dataType      收集的数据类型
   * @param collectorName 收集器名称
   * @param symbol        交易对
   * @return 是否取消成功
   */
  @Override
  public void unsubscribe(String collectorName, DataType dataType, String symbol,
                          Handler<AsyncResult<Void>> handler) {
    Collector collector = deployMap.get(collectorName);
    if (collector == null) {
      handler.handle(Future.failedFuture("collector not found"));
      return;
    }
    collector.unSubscribe(dataType, symbol, handler);
  }

  /**
   * 部署一个收集器
   *
   * @param collectorName 收集器名称
   * @return future
   */
  public Future<Void> deployCollector(String collectorName) {
    Promise<Void> promise = Promise.promise();
    deployCollector(collectorName, promise);
    return promise.future();
  }

  public Future<Void> deployCollectorEx(String collectorName, JsonObject jsonObject) {
    Promise<Void> promise = Promise.promise();
    deployCollectorEx(collectorName, jsonObject, promise);
    return promise.future();
  }

  /**
   * 启动收集器
   *
   * @param collectorName 收集器名称
   * @return future
   */
  public Future<Void> startCollector(String collectorName) {
    Promise<Void> promise = Promise.promise();
    startCollector(collectorName, promise);
    return promise.future();
  }

  /**
   * 订阅交易对
   *
   * @param dataType      收集的数据类型
   * @param collectorName 收集器名称
   * @param symbol        交易对
   * @return future
   */
  public Future<Void> subscribe(String collectorName, DataType dataType, String symbol) {
    Promise<Void> promise = Promise.promise();
    subscribe(collectorName, dataType, symbol, promise);
    return promise.future();
  }

  @Override
  public void onReceive(Collector from, DataType dataType, JsonObject obj) {
    log.info(obj.toString());
    counter++;
    sizeOfByte += obj.toBuffer().length();
    if (lastTime == null) {
      lastTime = System.currentTimeMillis();
    } else {
      long sec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - lastTime);
      if (sec > 0) {
        log.info("receive avg: {}/s  {}kb/s counter: {}", counter / sec, sizeOfByte / sec / 1024, counter);
      }
    }
    Topic topic;
    switch (dataType) {
      case KLINE: {
        topic = Topic.KLINE_TICK_TOPIC;
        break;
      }
      case DEPTH: {
        topic = Topic.DEPTH_CHART_TOPIC;
        break;
      }
      case TRADE_DETAIL: {
        topic = Topic.TRADE_DETAIL_TOPIC;
        break;
      }
      default:
        throw new IllegalStateException("Unexpected value: " + dataType);
    }
//    VertxUtil.asyncFastCallIgnoreRs(vertx, () -> {
    // 推送k线数据
    msgBus.publishIgnoreRs(topic, Message.withData(dataType, from.desc(), obj.encode()));
//    });
  }
}
