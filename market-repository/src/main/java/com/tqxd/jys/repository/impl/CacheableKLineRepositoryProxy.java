package com.tqxd.jys.repository.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.timeline.KLineMeta;
import com.tqxd.jys.timeline.cmd.AppendTickResult;
import com.tqxd.jys.timeline.cmd.AutoAggregateResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Objects;

/**
 * 可缓存的k线仓库代理
 *
 * @author lyqingye
 */
public class CacheableKLineRepositoryProxy implements KLineRepository{
  private KLineRepository cacheRepository;
  private KLineRepository persistRepository;

  public CacheableKLineRepositoryProxy (KLineRepository persistRepository) {
    this.cacheRepository = new InMemKLineRepository();
    this.persistRepository = Objects.requireNonNull(persistRepository);
  }

  @Override
  public void open(Vertx vertx, JsonObject config, Handler<AsyncResult<Void>> handler) {
    persistRepository.open(vertx,config, h -> {
      if (h.succeeded()) {
        cacheRepository.open(vertx,config, h1 -> {
          if (h1.succeeded()) {
            // TODO load snapshot to cache repository

          }else {
            handler.handle(Future.failedFuture(h1.cause()));
          }
        });
      }else {
        handler.handle(Future.failedFuture(h.cause()));
      }
    });

    cacheRepository.addListener(new KLineRepositoryListener() {
      @Override
      public void onAppendFinished(AppendTickResult rs) {
        if (!rs.getMeta().getPeriod().equals(Period._1_MIN)) {
          return;
        }
        KLineMeta meta = rs.getMeta();
        persistRepository.append(meta.getCommitIndex(), meta.getSymbol(),meta.getPeriod(),rs.getTick(), h -> {
          if (h.failed()) {
            h.cause().printStackTrace();
          }
        });

        if (rs.getDetail() != null) {
          persistRepository.putAggregate(meta.getSymbol(),rs.getDetail())
            .compose(h -> cacheRepository.putAggregate(meta.getSymbol(),rs.getDetail()))
            .onFailure(Throwable::printStackTrace);
        }
      }

      @Override
      public void onAutoAggregate(AutoAggregateResult rs) {
        KLineMeta meta = rs.getMeta();
        persistRepository.putAggregate(meta.getSymbol(),rs.getTick())
          .compose(h -> cacheRepository.putAggregate(meta.getSymbol(),rs.getTick()))
          .onFailure(Throwable::printStackTrace);
      }
    });
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
    cacheRepository.close(handler);
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