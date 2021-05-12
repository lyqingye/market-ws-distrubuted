package com.tqxd.jys.collectors.impl;

import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.openapi.payload.CollectorStatusDto;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;

/**
 * 三方数据收集器接口定义, 支持的功能如下：
 * 1. 部署数据采集器和取消部署
 * 2. 订阅交易对以及取消定义指定交易对
 * 3. 查看正在定义的交易对
 * 4. 开始和停止采集数据
 *
 * @author ex
 */
public interface Collector extends Verticle {
  /**
   * 返回当前收集器的名称
   *
   * @return 收集器名称
   */
  String name();

  /**
   * 描述一个收集器
   *
   * @return 收集器描述
   */
  default String desc() {
    return name();
  }

  default Future<Void> startFuture() {
    Promise<Void> promise = Promise.promise();
    try {
      this.start(promise);
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  default Future<Void> stopFuture() {
    Promise<Void> promise = Promise.promise();
    try {
      this.stop(promise);
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  /**
   * 订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @param handler  结果处理器
   */
  void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler);

  /**
   * 订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否订阅成功
   */
  default Future<Void> subscribe(DataType dataType, String symbol) {
    Promise<Void> promise = Promise.promise();
    this.subscribe(dataType, symbol, promise);
    return promise.future();
  }

  /**
   * 取消订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @param handler  结果处理器
   */
  void unSubscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler);

  /**
   * 取消订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否取消订阅成功
   */
  default Future<Void> unSubscribe(DataType dataType, String symbol) {
    Promise<Void> promise = Promise.promise();
    this.unSubscribe(dataType, symbol, promise);
    return promise.future();
  }

  /**
   * 获取当前正在订阅的信息
   */
  Map<DataType, List<String>> listSubscribedInfo();

  /**
   * 重新启动
   *
   * @param handler 结果处理器
   */
  void restart(Handler<AsyncResult<Void>> handler);

  default Future<Void> restart() {
    Promise<Void> promise = Promise.promise();
    this.restart(promise);
    return promise.future();
  }

  /**
   * 是否正在收集
   *
   * @return 是否正在收集
   */
  boolean isRunning();

  String deploymentID();

  void addDataReceiver(DataReceiver receiver);

  void unParkReceives(DataType dataType, JsonObject json);

  /**
   * 快照状态
   *
   * @return 状态快照
   */
  default CollectorStatusDto snapStatus() {
    CollectorStatusDto status = new CollectorStatusDto();
    status.setName(name());
    status.setDesc(desc());
    status.setRunning(isRunning());
    status.setSubscribedInfo(listSubscribedInfo());
    return status;
  }
}
