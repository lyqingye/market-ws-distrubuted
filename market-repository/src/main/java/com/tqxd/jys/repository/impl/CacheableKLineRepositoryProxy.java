package com.tqxd.jys.repository.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.timeline.KLineMeta;
import com.tqxd.jys.timeline.cmd.AppendTickResult;
import com.tqxd.jys.timeline.cmd.AutoAggregateResult;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 可缓存的k线仓库代理
 *
 * @author lyqingye
 */
public class CacheableKLineRepositoryProxy implements KLineRepository{
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
    long start = System.currentTimeMillis();
    persistRepository.open(vertx,config, persist -> {
      if (persist.succeeded()) {
        cacheRepository.open(vertx,config, cache -> {
          if (cache.succeeded()) {
            persistRepository.listSymbols()
              .compose(symbols -> {
                log.info("load symbols: {}",symbols);
                List<Future> allFutures = new ArrayList<>();
                // process all symbols
                for (String symbol : symbols) {
                  allFutures.add(persistRepository.loadSnapshot(symbol,Period._1_MIN)
                    .compose(snapshot -> {
                      log.info("load {} snapshot! committed index {}, size: {}",symbol,snapshot.getMeta().getCommittedIndex(),snapshot.getTickList().size());
                      List<Future> restoreFutures = new ArrayList<>();
                      // append all period
                      for (Period p : Period.values()) {
                        snapshot.getMeta().setPeriod(p);
                        restoreFutures.add(
                          cacheRepository.restoreWithSnapshot(snapshot)
                          .onSuccess(v -> log.info("restore {} {} snapshot success!",snapshot.getMeta().getSymbol(),p))
                        );
                      }
                      return CompositeFuture.all(restoreFutures);
                    }));
                }
                return CompositeFuture.all(allFutures);
              })
              .onSuccess(ignored -> {
                handler.handle(Future.succeededFuture());
                log.info("restore all snapshot success! using {}ms",System.currentTimeMillis() - start);
              })
              .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
          }else {
            handler.handle(Future.failedFuture(cache.cause()));
          }
        });
      }else {
        handler.handle(Future.failedFuture(persist.cause()));
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
