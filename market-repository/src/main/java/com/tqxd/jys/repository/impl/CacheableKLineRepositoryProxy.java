package com.tqxd.jys.repository.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.timeline.InMemKLineRepository;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.KLineRepositoryListener;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 可缓存的k线仓库代理
 *
 * @author lyqingye
 */
public class CacheableKLineRepositoryProxy implements KLineRepository {
  private static final Logger log = LoggerFactory.getLogger(CacheableKLineRepositoryProxy.class);
  private KLineRepository cacheRepository;
  private KLineRepository persistRepository;

  public CacheableKLineRepositoryProxy (KLineRepository persistRepository) {
    this.cacheRepository = new InMemKLineRepository();
    this.persistRepository = Objects.requireNonNull(persistRepository);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void open(Vertx vertx, JsonObject config, Handler<AsyncResult<Void>> handler) {
    persistRepository.open(vertx,config, persist -> {
      if (persist.succeeded()) {
        cacheRepository.open(vertx,config, cache -> {
          if (cache.succeeded()) {
            // cache从持久化库导入数据
            cacheRepository.importFrom(persistRepository)
              // 持久化仓库同步日志
              .compose(none -> persistRepository.syncAppendedFrom(cacheRepository))
              .onComplete(handler);
          }else {
            handler.handle(Future.failedFuture(cache.cause()));
          }
        });
      }else {
        handler.handle(Future.failedFuture(persist.cause()));
      }
    });
  }

  @Override
  public void listSymbols(Handler<AsyncResult<Set<String>>> handler) {
    cacheRepository.listSymbols(handler);
  }

  @Override
  public void loadSnapshot(String symbol, Period period, Handler<AsyncResult<KlineSnapshot>> handler) {
    cacheRepository.loadSnapshot(symbol, period, handler);
  }

  @Override
  public void restoreWithSnapshot(KlineSnapshot snapshot, Handler<AsyncResult<Void>> handler) {
    cacheRepository.restoreWithSnapshot(snapshot,handler);
  }

  @Override
  public void append(long commitIndex, String symbol, Period period, KlineTick tick, Handler<AsyncResult<Long>> handler) {
    if (!Period._1_MIN.equals(period)) {
      handler.handle(Future.failedFuture("redis repository only support 1min kline data!"));
      return;
    }
    // append all period
    for (Period p : Period.values()) {
      cacheRepository.append(commitIndex, symbol, p, tick, handler);
    }
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    cacheRepository.close()
      .compose(v -> persistRepository.close())
      .onComplete(handler);
  }

  @Override
  public void query(String symbol, Period period, long form, long to, Handler<AsyncResult<List<KlineTick>>> handler) {
    cacheRepository.query(symbol, period, form, to, handler);
  }

  @Override
  public void addListener(KLineRepositoryListener listener) {
    cacheRepository.addListener(listener);
  }

  @Override
  public void getAggregate(String symbol, Handler<AsyncResult<MarketDetailTick>> handler) {
    cacheRepository.getAggregate(symbol, handler);
  }

  @Override
  public void putAggregate(String symbol, MarketDetailTick tick, Handler<AsyncResult<Void>> handler) {
    cacheRepository.putAggregate(symbol, tick);
  }
}
