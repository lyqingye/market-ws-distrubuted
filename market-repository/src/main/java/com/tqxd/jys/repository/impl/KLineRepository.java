package com.tqxd.jys.repository.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Set;

/**
 * k线数据存储仓库
 *
 * @author lyqingye
 */
public interface KLineRepository {

  /**
   * 从另外一个仓库导入数据
   *
   * @param from 仓库
   * @param handler 结果处理器
   */
  default void importFrom (KLineRepository from, Handler<AsyncResult<Void>> handler) {}

  /**
   * 打开仓库
   *
   * @param vertx vertx
   * @param config 配置
   * @param handler 结果处理器
   */
  void open (Vertx vertx, JsonObject config, Handler<AsyncResult<Void>> handler);

  /**
   * 获取所有交易对
   *
   * @param handler 结果处理器
   */
  void listSymbols (Handler<AsyncResult<Set<String>>> handler);

  /**
   * 加载k线快照
   *
   * @param symbol 交易对
   * @param period {@link Period}
   * @param handler 结果处理器
   */
  void loadSnapshot (String symbol, Period period, Handler<AsyncResult<KlineSnapshot>> handler);

  /**
   * 根据快照进行恢复
   *
   * @param snapshot 快照
   * @param handler 结果处理器
   */
  void restoreWithSnapshot(KlineSnapshot snapshot, Handler<AsyncResult<Void>> handler);

  /**
   * 附加k线tick
   *
   * @param commitIndex 提交索引
   * @param symbol 交易对
   * @param period {@link Period}
   * @param tick k线tick
   * @param handler 结果处理器
   */
  void append (long commitIndex, String symbol, Period period,KlineTick tick, Handler<AsyncResult<Long>> handler);

  /**
   * 关闭与释放仓库资源
   *
   * @param handler 结果处理器
   */
  void close (Handler<AsyncResult<Void>> handler);

  /**
   * 将仓库缓存刷入持久化介质
   *
   * @param handler 结果处理器
   */
  default void flush (Handler<AsyncResult<Void>> handler) {}

  /**
   * k线历史查询
   *
   * @param symbol 交易对
   * @param period {@link Period}
   * @param form 开始时间戳
   * @param to 结束时间戳
   * @param handler 结果处理器
   */
  void query(String symbol,Period period,long form,long to,Handler<AsyncResult<List<KlineTick>>> handler);

  /**
   * 添加仓库监听器
   *
   * @param listener 监听器对象
   */
  void addListener(KLineRepositoryListener listener);

  /**
   * 查询24小时聚合数据
   *
   * @param symbol 交易对
   * @param handler 结果处理器
   */
  void getAggregate(String symbol, Handler<AsyncResult<MarketDetailTick>> handler);

  /**
   * 更新24小时聚合数据
   *
   * @param symbol 交易对
   * @param tick tick
   * @param handler 结果处理器
   */
  void putAggregate(String symbol, MarketDetailTick tick, Handler<AsyncResult<Void>> handler);

  //
  // future apis
  //

  default Future<Void> importFrom (KLineRepository from) {
    Promise<Void> promise = Promise.promise();
    importFrom(from,promise);
    return promise.future();
  }

  default Future<Void> open (Vertx vertx,JsonObject config) {
    Promise<Void> promise = Promise.promise();
    open(vertx,config,promise);
    return promise.future();
  }

  default Future<Set<String>> listSymbols () {
    Promise<Set<String>> promise = Promise.promise();
    listSymbols(promise);
    return promise.future();
  }

  default Future<Void> close () {
    Promise<Void> promise = Promise.promise();
    close(promise);
    return promise.future();
  }

  default Future<KlineSnapshot> loadSnapshot (String symbol,Period period) {
    Promise<KlineSnapshot> promise = Promise.promise();
    loadSnapshot(symbol,period,promise);
    return promise.future();
  }

  default Future<Void> restoreWithSnapshot(KlineSnapshot snapshot) {
    Promise<Void> promise = Promise.promise();
    restoreWithSnapshot(snapshot,promise);
    return promise.future();
  }

  default Future<Long> append (long commitIndex, String symbol, Period period, KlineTick tick) {
    Promise<Long> promise = Promise.promise();
    append(commitIndex,symbol,period,tick,promise);
    return promise.future();
  }

  default Future<Void> flush () {
    Promise<Void> promise = Promise.promise();
    flush(promise);
    return promise.future();
  }

  default Future<List<KlineTick>> query (String symbol,Period period, long from, long to) {
    Promise<List<KlineTick>> promise = Promise.promise();
    query(symbol, period, from, to,promise);
    return promise.future();
  }

  default Future<MarketDetailTick> getAggregate(String symbol) {
    Promise<MarketDetailTick> promise = Promise.promise();
    getAggregate(symbol,promise);
    return promise.future();
  }

  default Future<Void> putAggregate(String symbol,MarketDetailTick tick) {
    Promise<Void> promise = Promise.promise();
    putAggregate(symbol, tick,promise);
    return promise.future();
  }
}
