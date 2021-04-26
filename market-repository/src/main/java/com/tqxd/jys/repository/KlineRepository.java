package com.tqxd.jys.repository;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.repository.redis.RedisHelper;
import com.tqxd.jys.timeline.KlineTimeLine;
import com.tqxd.jys.timeline.KlineTimeLineMeta;
import com.tqxd.jys.timeline.KlineTimeManager;
import com.tqxd.jys.timeline.cmd.ApplyTickResult;
import com.tqxd.jys.timeline.cmd.CmdResult;
import com.tqxd.jys.utils.TimeUtils;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * k线数据的存储，使用redis作为存储
 * 1. 首先有一个set存放我们所有的k线数据的key {@link #SYMBOL_SET_KEY}
 * 里面的数据类似这样 [market.BTC-USDT.kline.1min,market.ETH-BTC.kline.1min]
 * <p>
 * 2. k线的快照我们需要一个元数据, 存放着快照对应的消息偏移等信息, 我们使用这个key作为前缀
 * {@link #METADATA_PREFIX}
 *
 * @author lyqingye
 */
public class KlineRepository {
  /**
   * 在redis维护一个集合存放各种key
   * [market.BTC-USDT.kline.1min,market.ETH-BTC.kline.1min]
   */
  public static final String SYMBOL_SET_KEY = "market:kline:symbols";
  /**
   * k线快照元数据的key的前缀，这个是一个hash数据结构
   * key: market:kline:metadata: + market.BTC-USDT.kline.1min
   * - commitIndex: 1
   * - updateTs: System.currentMills()
   */
  public static final String METADATA_PREFIX = "market:kline:metadata:";
  public static final String METADATA_COMMIT_INDEX = "commitIndex";
  public static final String METADATA_UPDATE_TS = "updateTs";
  public static final String KLINE_DETAIL_KEY = "market:kline:detail";
  private static final Logger log = LoggerFactory.getLogger(KlineRepository.class);
  /**
   * redis repo
   */
  private RedisHelper redis;

  /**
   * k线数据缓存管理器
   */
  private KlineTimeManager klineTimeManager;

  /**
   * 初始化仓库
   *
   * @param redis redis工具类
   */
  public static Future<KlineRepository> create(Vertx vertx, RedisHelper redis) {
    log.info("[Kline-Repository]: start load kline data!");
    KlineRepository self = new KlineRepository();
    self.redis = Objects.requireNonNull(redis);
    // 创建k线管理器
    self.klineTimeManager = KlineTimeManager.create(vertx, self::updateMarketDetail);
    Promise<KlineRepository> promise = Promise.promise();
    self.listKlineKeys()
        .onSuccess(keys -> {
          if (keys.isEmpty()) {
            log.info("[Kline-Repository]: symbols not found!");
            promise.complete(self);
            return;
          }
          log.info("[Kline-Repository]: found symbols: {}", keys);
          Future<Void> batchFuture = null;
          for (String key : keys) {
            if (batchFuture == null) {
              batchFuture = self.initKlineData(key);
            } else {
              batchFuture.compose(ar -> self.initKlineData(key));
            }
          }
          if (batchFuture == null) {
            promise.complete(self);
          } else {
            batchFuture.onSuccess(suc -> promise.complete(self))
                .onFailure(promise::fail);
          }
        })
        .onFailure(promise::fail);
    return promise.future();
  }

  public Future<Set<String>> listKlineKeys() {
    return redis.sMembers(SYMBOL_SET_KEY);
  }

  public void listKlineKeys(Handler<AsyncResult<Set<String>>> handler) {
    redis.sMembers(SYMBOL_SET_KEY, handler);
  }

  /**
   * 获取指定keyK线数据长度
   *
   * @param key     key
   * @param handler 结果处理器
   */
  public void sizeOfKlineTicks(String key, Handler<AsyncResult<Long>> handler) {
    redis.zCard(key, handler);
  }

  /**
   * 获取指定keyK线数据长度
   *
   * @param key key
   * @return future
   */
  public Future<Long> sizeOfKlineTicks(String key) {
    Promise<Long> promise = Promise.promise();
    sizeOfKlineTicks(key, promise);
    return promise.future();
  }

  /**
   * 根据key获取指定区间内的交易ticks
   *
   * @param key     key
   * @param start   开始索引 0开始
   * @param stop    结束索引 size - 1
   * @param handler 结果处理器
   */
  public void listKlineTicksLimit(String key, long start, long stop, Handler<AsyncResult<List<String>>> handler) {
    redis.zRange(key, start, stop, handler);
  }

  /**
   * 根据key获取指定区间内的交易ticks
   *
   * @param key   key
   * @param start 开始索引 0开始
   * @param stop  结束索引 size - 1
   * @return future
   */
  public Future<List<String>> listKlineTicksLimit(String key, long start, long stop) {
    Promise<List<String>> promise = Promise.promise();
    listKlineTicksLimit(key, start, stop, promise);
    return promise.future();
  }

  public void getCommittedIndex(String key, Handler<AsyncResult<Long>> handler) {
    redis.hGet(key, METADATA_COMMIT_INDEX, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(Long.valueOf((String) ar.result())));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public Future<Long> getCommittedIndex(String key) {
    Promise<Long> promise = Promise.promise();
    getCommittedIndex(key, promise);
    return promise.future();
  }

  public void getKlineSnapshot(String klineKey, Handler<AsyncResult<KlineSnapshot>> handler) {
    getCommittedIndex(METADATA_PREFIX + klineKey)
        .compose(committedIndex -> {
          return sizeOfKlineTicks(klineKey)
              .compose(size -> {
                KlineSnapshot snapshot = new KlineSnapshot();
                snapshot.setCommittedIndex(committedIndex);
                snapshot.setKlineKey(klineKey);
                snapshot.setPeriod(Period._1_MIN);
                if (size != null && size > 0) {
                  // 只截取最新的部分
                  long start = 0;
                  if (size >= Period._1_MIN.getNumOfPeriod()) {
                    start = size - Period._1_MIN.getNumOfPeriod();
                  }
                  return listKlineTicksLimit(klineKey, start, -1)
                      .compose(ticks -> {
                        if (!ticks.isEmpty()) {
                          try {
                            List<KlineTick> tickList = new ArrayList<>(ticks.size());
                            for (String tickJson : ticks) {
                              tickList.add(Json.decodeValue(tickJson, KlineTick.class));
                            }
                            snapshot.setTickList(tickList);
                          } catch (Exception ex) {
                            ex.printStackTrace();
                          }
                        }
                        return Future.succeededFuture(snapshot);
                      });
                } else {
                  return Future.succeededFuture(snapshot);
                }
              });
        })
        .onSuccess(snapshot -> handler.handle(Future.succeededFuture(snapshot)))
        .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  public Future<KlineSnapshot> getKlineSnapshot(String klineKey) {
    Promise<KlineSnapshot> promise = Promise.promise();
    getKlineSnapshot(klineKey, promise);
    return promise.future();
  }

  // ----------------------------------------------------------------------------------------------------------

  /**
   * 更新k线
   */
  private void updateAsync(ApplyTickResult data, long commitIndex, long ts, Handler<AsyncResult<Void>> handler) {
    KlineTick tick = data.getTick();
    KlineTimeLineMeta meta = data.getMeta();
    String klineKey = meta.getKlineKey();
    String detailKey = meta.getDetailKey();
    // 构造redis命令
    List<Request> batchCmd = new ArrayList<>(6);

    // 更新key集合信息
    batchCmd.add(Request.cmd(Command.SADD).arg(SYMBOL_SET_KEY).arg(klineKey));
    long time = TimeUtils.alignWithPeriod(tick.getTime(), Period._1_MIN.getMill());

    // 移除原来的tick
    batchCmd.add(Request.cmd(Command.ZREMRANGEBYSCORE).arg(klineKey).arg(time).arg(time));

    // 替换现在的tick
    batchCmd.add(Request.cmd(Command.ZADD).arg(klineKey).arg(time).arg(Json.encode(tick)));

    // 更新快照元数据
    batchCmd.add(Request.cmd(Command.HSET).arg(METADATA_PREFIX + klineKey).arg(METADATA_COMMIT_INDEX).arg(commitIndex));
    batchCmd.add(Request.cmd(Command.HSET).arg(METADATA_PREFIX + klineKey).arg(METADATA_UPDATE_TS).arg(ts));
    batchCmd.add(Request.cmd(Command.HSET).arg(KLINE_DETAIL_KEY).arg(detailKey).arg(Json.encode(TemplatePayload.of(detailKey, tick))));
    // 批量执行
    redis.batch(batchCmd, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void updateMarketDetail(KlineTimeLineMeta meta, MarketDetailTick tick) {
    redis.hSet(KLINE_DETAIL_KEY, meta.getDetailKey(), Json.encode(TemplatePayload.of(meta.getDetailKey(), tick)), ar -> {
      if (ar.failed()) {
        ar.cause().printStackTrace();
      }
    });
  }

  /**
   * 需要自己更新数据
   */
  public void forUpdateKline(long commitIndex, long ts, TemplatePayload<KlineTick> payload) {
    if (payload != null) {
      String sub = payload.getCh();
      KlineTick tick = payload.getTick();
      if (tick != null) {
        CmdResult<ApplyTickResult> updateResult = klineTimeManager.applyTick(sub, Period._1_MIN, commitIndex, tick);
        ApplyTickResult updatedResult = null;
        try {
          updatedResult = updateResult.get();
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
          return;
        }
        if (updateResult.isSuccess()) {
          // 异步更新到redis
          this.updateAsync(updatedResult, commitIndex, ts, ar -> {
            if (ar.failed()) {
              ar.cause().printStackTrace();
            }
          });
        } else {
          log.warn("[Kline-Repository]: update kline tick fail! reason: {}, commitIndex: {} payload: {}", updateResult.getReason(), commitIndex, Json.encode(payload));
        }
      }
    } else {
      log.info("[Kline-Repository]: payload is null! message index: {}", commitIndex);
    }
  }

  /**
   * 初始化k线数据
   *
   * @param klineKey key
   * @return future
   */
  private Future<Void> initKlineData(String klineKey) {
    long startTime = System.currentTimeMillis();
    return getKlineSnapshot(klineKey)
        .compose(snapshot -> {
          KlineTimeLine timeLine = klineTimeManager.getOrCreate(klineKey, Period._1_MIN);
          try {
            timeLine.applySnapshot(snapshot.getCommittedIndex(), snapshot.getTickList()).get();
          } catch (Exception ex) {
            ex.printStackTrace();
          }
          log.info("[Kline-Repository]: init kline: {} size: {} using: {}ms", klineKey, snapshot.getTickList().size(), System.currentTimeMillis() - startTime);
          return Future.succeededFuture();
        });
  }
}
