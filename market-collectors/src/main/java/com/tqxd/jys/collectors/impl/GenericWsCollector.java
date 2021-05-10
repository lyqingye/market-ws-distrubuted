package com.tqxd.jys.collectors.impl;

import com.tqxd.jys.constance.DataType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author yjt
 * @since 2020/10/10 上午9:54
 */
public abstract class GenericWsCollector implements Collector {
  Logger log = LoggerFactory.getLogger(GenericWsCollector.class);

  /**
   * 已经订阅的信息
   */
  private Map<DataType, List<String>> subscribed = new HashMap<>(16);

  /**
   * vertx 实例
   */
  private Vertx vertx;

  /**
   * 是否正在运行
   */
  private volatile boolean isRunning;

  /**
   * 是否已经部署
   */
  private volatile boolean isDeployed;

  /**
   * 上一次收到消息的时间，用于空闲链路检测
   */
  private long lastReceiveTimestamp;

  /**
   * 空闲链路判定时间 （超过这个时间就会被判定为空闲链路）
   */
  private long idleTime = TimeUnit.SECONDS.toMillis(5);

  /**
   * 空闲链路检测定时器
   */
  private long idleCheckerTimerId;

  /**
   * 部署一个收集器
   *
   * @param vertx    vertx 实例
   * @param consumer 数据消费器
   * @param args     附加参数 (可以为空)
   * @return 是否部署成功
   * @throws Exception 如果部署失败
   */
  @Override
  public boolean deploy(Vertx vertx,
                        BiConsumer<DataType, JsonObject> consumer,
                        JsonObject args) {
    if (vertx == null) {
      return false;
    }
    this.vertx = vertx;

    if (this.isDeployed) {
      return true;
    }
    this.isRunning = false;
    this.isDeployed = true;

    return true;
  }

  /**
   * 取消部署收集器
   *
   * @param args 附加参数可以为空
   * @return 如果取消部署失败
   * @throws Exception 如果取消部署失败
   */
  @Override
  public boolean unDeploy(JsonObject args) {
    if (isDeployed) {
      try {
        if (this.ws() != null && !this.ws().isClosed()) {
          this.ws().close();
        }
        this.isRunning = false;
        this.isDeployed = false;
        return true;
      } catch (Exception ex) {
        ex.printStackTrace();
      }
      return false;
    }
    return true;
  }

  /**
   * 订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否订阅成功
   */
  @Override
  public boolean subscribe(DataType dataType, String symbol) {
    if (!this.isRunning)
      return false;
    List<String> symbols;
    if ((symbols = subscribed.get(dataType)) != null) {
      for (String exist : symbols) {
        if (exist.equals(symbol)) {
          return true;
        }
      }
    }
    subscribed.computeIfAbsent(dataType, k -> new ArrayList<>()).add(symbol);
    return true;
  }

  /**
   * 取消订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否取消订阅成功
   */
  @Override
  public boolean unSubscribe(DataType dataType, String symbol) {
    List<String> symbols = subscribed.get(dataType);
    if (symbols == null) {
      return true;
    }
    Iterator<String> it = symbols.iterator();
    while (it.hasNext()) {
      String obj = it.next();
      if (obj.equals(symbol)) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  /**
   * 获取当前正在订阅的交易对
   *
   * @return 当前正在订阅的信息, key为数据收集类型, value为交易对列表
   */
  @Override
  public Map<DataType, List<String>> listSubscribedInfo() {
    return subscribed;
  }

  /**
   * 开启收集数据
   *
   * @param handler 回调
   */
  @Override
  public void start(Handler<AsyncResult<Boolean>> handler) {

    if (this.isRunning) {
      handler.handle(Future.succeededFuture(true));
    }

    try {
      if (this.ws() != null && !this.ws().isClosed()) {
        this.ws().close();
      }

      this.isRunning = true;

      // 部署 websocket
      handler.handle(Future.succeededFuture(true));
    } catch (Exception ex) {
      this.isRunning = false;
      handler.handle(Future.failedFuture(ex));
    }
  }

  /**
   * 停止数据收集
   *
   * @return 是否停止成功
   */
  @Override
  public void stop(Handler<AsyncResult<Void>> handler) {
    if (isRunning()) {
      try {
        this.isRunning = false;
        if (!this.ws().isClosed()) {
          this.ws().close().onComplete(handler);
        }
      } catch (Exception ex) {
        handler.handle(Future.failedFuture(ex));
      }
    }
    handler.handle(Future.succeededFuture());
  }

  /**
   * 是否正在收集
   *
   * @return 是否正在收集
   */
  @Override
  public boolean isRunning() {
    return this.isRunning;
  }

  /**
   * 是否已经部署
   *
   * @return 是否已经部署
   */
  @Override
  public boolean isDeployed() {
    return isDeployed;
  }

  /**
   * 获取websocket实例
   *
   * @return 实例
   */
  public abstract WebSocket ws();

  /**
   * 启动空闲检测
   */
  public void startIdleChecker() {
    idleCheckerTimerId = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(1), timeId -> {
      if (isRunning()) {
        if (System.currentTimeMillis() - lastReceiveTimestamp >= idleTime) {
          log.info("[Collectors]: collector {} idle detected, try to restart! ", this.name());
          stop(ar -> {
            if (ar.succeeded()) {
              log.info("[Collectors]: stop collector: {} success!", this.name());
              start(startAr -> {
                if (startAr.succeeded()) {
                  log.info("[Collectors]: start collector: {} success!", this.name());
                } else {
                  startAr.cause().printStackTrace();
                }
              });
            } else {
              ar.cause().printStackTrace();
              log.error("[Collectors]: stop collector: {}  fail!", this.name());
            }
          });
        }
      }
    });
  }

  /**
   * 刷新上一次收到消息的时间, 用于空闲链路检测
   */
  public void refreshLastReceiveTime() {
    lastReceiveTimestamp = System.currentTimeMillis();
  }

  /**
   * 停止空闲检测
   */
  public void stopIdleChecker() {
    if (idleCheckerTimerId != -1) {
      vertx.cancelTimer(idleCheckerTimerId);
      idleCheckerTimerId = -1;
    }
  }
}
