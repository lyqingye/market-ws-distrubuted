package com.tqxd.jys.timeline;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
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
   * @param from    仓库
   * @param handler 结果处理器
   */
  default void importFrom(KLineRepository from, Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 与指定库同步附加日志
   *
   * @param target  目标库
   * @param handler 结果处理器
   */
  default void syncAppendedFrom(KLineRepository target, Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 打开仓库
   *
   * @param vertx   vertx
   * @param config  配置
   * @param handler 结果处理器
   */
  default void open(Vertx vertx, JsonObject config, Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 获取所有交易对
   *
   * @param handler 结果处理器
   */
  default void listSymbols (Handler<AsyncResult<Set<String>>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 加载k线快照
   *
   * @param symbol 交易对
   * @param period {@link Period}
   * @param handler 结果处理器
   */
  default void loadSnapshot (String symbol, Period period, Handler<AsyncResult<KlineSnapshot>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 根据快照进行恢复
   *
   * @param snapshot 快照
   * @param handler 结果处理器
   */
  default void restoreWithSnapshot(KlineSnapshot snapshot, Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 附加k线tick
   *
   * @param commitIndex 提交索引
   * @param symbol 交易对
   * @param period {@link Period}
   * @param tick k线tick
   * @param handler 结果处理器
   */
  default void append (long commitIndex, String symbol, Period period,KlineTick tick, Handler<AsyncResult<Long>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 关闭与释放仓库资源
   *
   * @param handler 结果处理器
   */
  default void close (Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 将仓库缓存刷入持久化介质
   *
   * @param handler 结果处理器
   */
  default void flush(Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 刷盘策略，默认为 SYNC
   *
   * @return 刷盘策略
   */
  default FlushMode getFlushMode() {
    return FlushMode.SYNC;
  }

  /**
   * k线历史查询
   *
   * @param symbol  交易对
   * @param period  {@link Period}
   * @param form    开始时间戳
   * @param to      结束时间戳
   * @param handler 结果处理器
   */
  default void query(String symbol,Period period,long form,long to,Handler<AsyncResult<List<KlineTick>>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 添加仓库监听器
   *
   * @param listener 监听器对象
   */
  default void addListener(KLineRepositoryListener listener) {}

  /**
   * 查询24小时聚合数据
   *
   * @param symbol  交易对
   * @param handler 结果处理器
   */
  default void getAggregate(String symbol, Handler<AsyncResult<KlineTick>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

  /**
   * 更新24小时聚合数据
   *
   * @param symbol  交易对
   * @param tick    tick
   * @param handler 结果处理器
   */
  default void putAggregate(String symbol, KlineTick tick, Handler<AsyncResult<Void>> handler) {
    handler.handle(Future.failedFuture("not implementation!"));
  }

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

  default Future<List<KlineTick>> query(String symbol, Period period, long from, long to) {
    Promise<List<KlineTick>> promise = Promise.promise();
    query(symbol, period, from, to, promise);
    return promise.future();
  }

  default Future<KlineTick> getAggregate(String symbol) {
    Promise<KlineTick> promise = Promise.promise();
    getAggregate(symbol, promise);
    return promise.future();
  }

  default Future<Void> putAggregate(String symbol, KlineTick tick) {
    Promise<Void> promise = Promise.promise();
    putAggregate(symbol, tick, promise);
    return promise.future();
  }

  default Future<Void> syncAppendedFrom(KLineRepository target) {
    Promise<Void> promise = Promise.promise();
    syncAppendedFrom(target, promise);
    return promise.future();
  }
}
