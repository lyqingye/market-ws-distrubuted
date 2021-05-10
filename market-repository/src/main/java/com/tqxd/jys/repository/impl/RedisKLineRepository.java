package com.tqxd.jys.repository.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.openapi.payload.KlineSnapshotMeta;
import com.tqxd.jys.repository.redis.RedisHelper;
import com.tqxd.jys.repository.redis.RedisKeyHelper;
import com.tqxd.jys.timeline.KLineMeta;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.KLineRepositoryListener;
import com.tqxd.jys.timeline.cmd.AppendTickResult;
import com.tqxd.jys.timeline.cmd.Auto24HourStatisticsResult;
import com.tqxd.jys.utils.TimeUtils;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * k线redis仓库
 *
 * @author lyqingye
 */
public class RedisKLineRepository implements KLineRepository {
  private static final Logger log = LoggerFactory.getLogger(RedisKLineRepository.class);
  private RedisHelper redis;
  private List<KLineRepositoryListener> listeners = new ArrayList<>();

  @Override
  public void open(Vertx vertx, JsonObject config, Handler<AsyncResult<Void>> handler) {
    if (redis != null) {
      handler.handle(Future.succeededFuture());
    }
    String redisConnString = "redis://localhost:6379/7";
    if (config != null) {
      redisConnString = VertxUtil.jsonGetValue(config, "market.repository.redis.connectionString", String.class, redisConnString);
    }
    RedisHelper.create(vertx, redisConnString)
      .onSuccess(redis -> {
        this.redis = redis;
        handler.handle(Future.succeededFuture());
      })
      .onFailure(throwable -> {
        handler.handle(Future.failedFuture(throwable));
      });
  }

  @Override
  public void listSymbols(Handler<AsyncResult<Set<String>>> handler) {
    redis.sMembers(RedisKeyHelper.getSymbolsKey(), handler);
  }

  @Override
  public void loadSnapshot(String symbol, Period period, Handler<AsyncResult<KlineSnapshot>> handler) {
    loadSnapshotMeta(symbol)
      .compose
        (
            meta -> sizeOfKlineTicks(symbol, period)
                .compose(size -> {
                  KlineSnapshot snapshot = new KlineSnapshot();
                  snapshot.setMeta(meta);
                  if (size != null && size > 0) {
                    long start = 0;
                    if (size >= Period._1_MIN.getNumOfPeriod()) {
                      start = size - Period._1_MIN.getNumOfPeriod();
                    }
                    return listKlineTicksLimit(symbol, period, start, -1)
                        .compose(ticks -> {
                          snapshot.setTickList(ticks);
                          return Future.succeededFuture(snapshot);
                        });
              } else {
                return Future.succeededFuture(snapshot);
              }
            })
        )
      .onSuccess(snapshot -> handler.handle(Future.succeededFuture(snapshot)))
      .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  private Future<List<KlineTick>> listKlineTicksLimit(String symbol, Period period, long start, int stop) {
    Promise<List<KlineTick>> promise = Promise.promise();
    redis.zRange(RedisKeyHelper.toKlineDataKey(symbol, period), start, stop, ar -> {
      if (ar.succeeded()) {
        List<String> ticks = ar.result();
        if (!ticks.isEmpty()) {
          try {
            List<KlineTick> tickList = new ArrayList<>(ticks.size());
            for (String tickJson : ticks) {
              tickList.add(Json.decodeValue(tickJson, KlineTick.class));
            }
            promise.complete(tickList);
          } catch (Exception ex) {
            promise.fail(ex);
          }
        } else {
          promise.complete(Collections.emptyList());
        }
      }
    });
    return promise.future();
  }

  private Future<KlineSnapshotMeta> loadSnapshotMeta(String symbol) {
    Promise<KlineSnapshotMeta> promise = Promise.promise();
    redis.get(RedisKeyHelper.toKlineMetaKey(symbol), ar -> {
      if (ar.succeeded()) {
        promise.complete(Json.decodeValue(ar.result(), KlineSnapshotMeta.class));
      } else {
        promise.fail(ar.cause());
      }
    });
    return promise.future();
  }

  private Future<Long> sizeOfKlineTicks(String symbol, Period period) {
    Promise<Long> promise = Promise.promise();
    redis.zCard(RedisKeyHelper.toKlineDataKey(symbol, period), promise);
    return promise.future();
  }

  @Override
  public void restoreWithSnapshot(KlineSnapshot snapshot, Handler<AsyncResult<Void>> handler) {

  }

  @Override
  public void append(long commitIndex, String symbol, Period period, KlineTick tick, Handler<AsyncResult<Long>> handler) {
    String klineKey = RedisKeyHelper.toKlineDataKey(symbol, period);
    // 构造redis命令
    List<Request> batchCmd = new ArrayList<>(4);

    // 更新key集合信息
    batchCmd.add(
      Request.cmd(Command.SADD)
        .arg(RedisKeyHelper.getSymbolsKey())
        .arg(symbol)
    );

    // 更新k线tick
    long time = TimeUtils.alignWithPeriod(tick.getTime(), period.getMill());
    batchCmd.add(
      Request.cmd(Command.ZREMRANGEBYSCORE)
        .arg(klineKey)
        .arg(time)
        .arg(time)
    );
    batchCmd.add(
      Request.cmd(Command.ZADD)
        .arg(klineKey)
        .arg(time)
        .arg(Json.encode(tick))
    );

    // 更新快照元数据
    KlineSnapshotMeta snapshotMeta = new KlineSnapshotMeta();
    snapshotMeta.setTs(System.currentTimeMillis());
    snapshotMeta.setCommittedIndex(commitIndex);
    snapshotMeta.setPeriod(period);
    snapshotMeta.setSymbol(symbol);
    batchCmd.add(
      Request.cmd(Command.SET)
        .arg(RedisKeyHelper.toKlineMetaKey(symbol))
        .arg(Json.encode(snapshotMeta))
    );

    // 批量执行
    redis.batch(batchCmd, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    try {
      redis.close();
      handler.handle(Future.succeededFuture());
    } catch (Exception e) {
      handler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void query(String symbol, Period period, long from, long to, Handler<AsyncResult<List<KlineTick>>> handler) {
    redis.zRangeByScore(RedisKeyHelper.toKlineDataKey(symbol, period), from, to, ar -> {
      if (ar.succeeded()) {
        List<String> ticks = ar.result();
        if (!ticks.isEmpty()) {
          try {
            List<KlineTick> tickList = new ArrayList<>(ticks.size());
            for (String tickJson : ticks) {
              tickList.add(Json.decodeValue(tickJson, KlineTick.class));
            }
            handler.handle(Future.succeededFuture(tickList));
          } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
          }
        } else {
          handler.handle(Future.succeededFuture(Collections.emptyList()));
        }
      }
    });
  }

  @Override
  public void addListener(KLineRepositoryListener listener) {
    listeners.add(Objects.requireNonNull(listener));
  }

  @Override
  public void getAggregate(String symbol, Handler<AsyncResult<KlineTick>> handler) {
    redis.get(RedisKeyHelper.toMarketDetailKey(symbol), ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(Json.decodeValue(ar.result(), KlineTick.class)));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public void putAggregate(String symbol, KlineTick tick, Handler<AsyncResult<Void>> handler) {
    redis.set(RedisKeyHelper.toMarketDetailKey(symbol), Json.encode(tick), handler);
  }

  @Override
  public void syncAppendedFrom(KLineRepository target, Handler<AsyncResult<Void>> handler) {
    target.addListener(new KLineRepositoryListener() {
      @Override
      public void onAppendFinished(AppendTickResult rs) {
        KLineMeta meta = rs.getMeta();
        append(meta.getCommitIndex(), meta.getSymbol(), meta.getPeriod(), rs.getTick(), h -> {
          if (h.failed()) {
            h.cause().printStackTrace();
          }
        });

        if (rs.getDetail() != null) {
          putAggregate(meta.getSymbol(), rs.getDetail())
            .onFailure(Throwable::printStackTrace);
        }
      }

      @Override
      public void onAutoAggregate(Auto24HourStatisticsResult rs) {
        KLineMeta meta = rs.getMeta();
        putAggregate(meta.getSymbol(), rs.getTick())
            .onFailure(Throwable::printStackTrace);
      }
    });
    handler.handle(Future.succeededFuture());
  }
}
