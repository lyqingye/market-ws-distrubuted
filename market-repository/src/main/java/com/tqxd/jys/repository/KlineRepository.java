package com.tqxd.jys.repository;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.openapi.payload.KlineSnapshotMeta;
import com.tqxd.jys.repository.redis.RedisHelper;
import com.tqxd.jys.repository.redis.RedisKeyHelper;
import com.tqxd.jys.timeline.KLineManager;
import com.tqxd.jys.timeline.KLineMeta;
import com.tqxd.jys.timeline.cmd.ApplyTickResult;
import com.tqxd.jys.utils.ChannelUtil;
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

/**
 * k线数据的存储，使用redis作为存储
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
  private KLineManager klineManager;

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
    self.klineManager = KLineManager.create();
    self.klineManager.setOutResultConsumer(self::klineDataConsumer);
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
    return redis.sMembers(RedisKeyHelper.getSymbolsKey());
  }

  public void listKlineKeys(Handler<AsyncResult<Set<String>>> handler) {
    redis.sMembers(RedisKeyHelper.getSymbolsKey(), handler);
  }

  /**
   * 获取指定交易对K线数据长度
   *
   * @param symbol  交易对
   * @param handler 结果处理器
   */
  public void sizeOfKlineTicks(String symbol, Handler<AsyncResult<Long>> handler) {
    redis.zCard(RedisKeyHelper.toKlineDataKey(symbol), handler);
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
   * 根据交易对获取指定区间内的交易ticks
   *
   * @param symbol  交易对
   * @param start   开始索引 0开始
   * @param stop    结束索引 size - 1
   * @param handler 结果处理器
   */
  public void listKlineTicksLimit(String symbol, long start, long stop, Handler<AsyncResult<List<String>>> handler) {
    redis.zRange(RedisKeyHelper.toKlineDataKey(symbol), start, stop, handler);
  }

  /**
   * 根据交易对获取指定区间内的交易ticks
   *
   * @param symbol 交易对
   * @param start  开始索引 0开始
   * @param stop   结束索引 size - 1
   * @return future
   */
  public Future<List<String>> listKlineTicksLimit(String symbol, long start, long stop) {
    Promise<List<String>> promise = Promise.promise();
    listKlineTicksLimit(symbol, start, stop, promise);
    return promise.future();
  }

  public Future<KlineSnapshotMeta> getKlineSnapshotMeta(String symbol) {
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

  public void getKlineSnapshot(String symbol, Handler<AsyncResult<KlineSnapshot>> handler) {
    getKlineSnapshotMeta(symbol)
        .compose(meta -> {
          return sizeOfKlineTicks(symbol)
              .compose(size -> {
                KlineSnapshot snapshot = new KlineSnapshot();
                snapshot.setMeta(meta);
                if (size != null && size > 0) {
                  // 只截取最新的部分
                  long start = 0;
                  if (size >= Period._1_MIN.getNumOfPeriod()) {
                    start = size - Period._1_MIN.getNumOfPeriod();
                  }
                  return listKlineTicksLimit(symbol, start, -1)
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

  public Future<KlineSnapshot> getKlineSnapshot(String symbol) {
    Promise<KlineSnapshot> promise = Promise.promise();
    getKlineSnapshot(symbol, promise);
    return promise.future();
  }

  // ----------------------------------------------------------------------------------------------------------

  /**
   * 更新k线
   */
  private void applyTickResultAsync(ApplyTickResult data, Handler<AsyncResult<Void>> handler) {
    KlineTick tick = data.getTick();
    KLineMeta meta = data.getMeta();
    String klineKey = RedisKeyHelper.toKlineDataKey(meta.getSymbol());
    // 构造redis命令
    List<Request> batchCmd = new ArrayList<>(5);

    // 更新key集合信息
    batchCmd.add(Request.cmd(Command.SADD).arg(RedisKeyHelper.getSymbolsKey()).arg(meta.getSymbol()));
    long time = TimeUtils.alignWithPeriod(tick.getTime(), Period._1_MIN.getMill());

    // 更新k线tick
    batchCmd.add(Request.cmd(Command.ZREMRANGEBYSCORE).arg(klineKey).arg(time).arg(time));
    batchCmd.add(Request.cmd(Command.ZADD).arg(klineKey).arg(time).arg(Json.encode(tick)));

    // 更新快照元数据
    KlineSnapshotMeta snapshotMeta = new KlineSnapshotMeta();
    snapshotMeta.setTs(System.currentTimeMillis());
    snapshotMeta.setCommittedIndex(meta.getCommitIndex());
    snapshotMeta.setPeriod(meta.getPeriod());
    snapshotMeta.setSymbol(meta.getSymbol());
    batchCmd.add(Request.cmd(Command.SET).arg(RedisKeyHelper.toKlineMetaKey(meta.getSymbol())).arg(Json.encode(snapshotMeta)));

    // 更新市场概要
    batchCmd.add(Request.cmd(Command.SET).arg(RedisKeyHelper.toMarketDetailKey(meta.getSymbol())).arg(Json.encode(data.getDetail())));

    // 批量执行
    redis.batch(batchCmd, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void klineDataConsumer(Object obj) {
    if (obj instanceof TemplatePayload) {
      if (((TemplatePayload) obj).getTick() instanceof MarketDetailTick) {
        TemplatePayload<MarketDetailTick> payload = (TemplatePayload<MarketDetailTick>) obj;
        redis.set(RedisKeyHelper.toMarketDetailKey(ChannelUtil.getSymbol(payload.getCh())), Json.encode(obj), ar -> {
          if (ar.failed()) {
            ar.cause().printStackTrace();
          }
        });
      }
    } else if (obj instanceof ApplyTickResult) {
      ApplyTickResult result = (ApplyTickResult) obj;
      applyTickResultAsync(result, ar -> {
        if (ar.failed()) {
          log.warn("[Kline-Repository]: update kline tick fail! reason: {}, commitIndex: {} payload: {}", ar.cause().getMessage(), result.getMeta().getCommitIndex(), Json.encode(obj));
          ar.cause().printStackTrace();
        }
      });
    } else {
      log.warn("unknown data: {}", obj);
    }
  }

  /**
   * 需要自己更新数据
   */
  public void forUpdateKline(long commitIndex, long ts, TemplatePayload<KlineTick> payload) {
    if (payload != null) {
      String sub = payload.getCh();
      KlineTick tick = payload.getTick();
      if (tick != null) {
        klineManager.applyTick(ChannelUtil.getSymbol(sub), Period._1_MIN, commitIndex, tick, h -> {
          if (h.failed()) {
            log.warn("[Kline-Repository]: apply kline tick fail! reason: {}, commitIndex: {} payload: {}", h.cause().getMessage(), commitIndex, Json.encode(payload));
            h.cause().printStackTrace();
          }
        });
      }
    } else {
      log.info("[Kline-Repository]: payload is null! message index: {}", commitIndex);
    }
  }

  /**
   * 初始化k线数据
   *
   * @param symbol key
   * @return future
   */
  private Future<Void> initKlineData(String symbol) {
    long startTime = System.currentTimeMillis();
    return getKlineSnapshot(symbol)
        .compose(snapshot -> {
          klineManager.applySnapshot(snapshot, h -> {
            if (h.failed()) {
              h.cause().printStackTrace();
            } else {
              log.info("[Kline-Repository]: init kline: {} size: {} using: {}ms", symbol, snapshot.getTickList().size(), System.currentTimeMillis() - startTime);
            }
          });
          return Future.succeededFuture();
        });
  }
}
