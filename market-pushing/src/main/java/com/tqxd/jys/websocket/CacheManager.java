package com.tqxd.jys.websocket;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.depth.DepthTick;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTickData;
import com.tqxd.jys.timeline.KLineManager;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.websocket.processor.Response;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import org.apache.curator.framework.imps.GzipCompressionProvider;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 缓存管理器, 直接面向用户数据
 *
 * @author lyqingye
 */
public class CacheManager {
  /**
   * 市场概括缓存
   */
  private Map<String, MarketDetailTick> marketDetailCache = new HashMap<>();

  /**
   * 成交记录缓存
   */
  private Map<String, List<TradeDetailTickData>> tradeDetailHistory = new HashMap<>();
  private Map<String, List<TradeDetailTickData>> tradeDetailHistoryTemp = new HashMap<>();

  /**
   * 深度缓存, 默认20档
   */
  private Map<String, DepthTick> marketDepthData = new HashMap<>();

  /**
   * k线管理器
   */
  private KLineManager kLineManager;

  public CacheManager (KLineManager kLineManager) {
    this.kLineManager = Objects.requireNonNull(kLineManager);
  }

  /**
   * 查询全量深度
   *
   * @param symbol 交易对
   * @param level  深度等级 {@link DepthLevel}
   * @return null or 深度数据
   */
  public @Nullable Buffer reqDepth(@NonNull String symbol, @NonNull DepthLevel level,int size) {


    return null;
  }

  /**
   * 查询全量成交明细
   *
   * @param symbol 交易对
   * @param size   条数，默认30
   * @return null or 成交明细数据
   */
  public @Nullable List<TradeDetailTickData> reqTradeDetail(@NonNull String symbol, int size) {
    List<TradeDetailTickData> data = tradeDetailHistory.get(ChannelUtil.buildTradeDetailChannel(symbol));
    if (data != null) {
      List<TradeDetailTickData> subList;
      if (size > data.size()) {
        subList = data;
      }else {
        subList = data.subList(0, size);
      }
      return subList;
    }
    return null;
  }

  /**
   * 查询24小时市场概要
   *
   * @param symbol 交易对
   * @return null or 市场概要数据
   */
  public @Nullable MarketDetailTick reqMarketDetail(@NonNull String symbol) {
     return marketDetailCache.get(ChannelUtil.buildMarketDetailChannel(symbol));
  }

  /**
   * 查询分时图
   *
   * @param symbol  交易对
   * @param from    开始时间
   * @param to      结束时间
   * @param handler 异步结果处理器
   */
  public void reqTimeSharing(@NonNull String symbol, long from, long to,
                             @NonNull Handler<AsyncResult<Buffer>> handler) {
    // TODO 暂不支持
  }

  /**
   * 查询k线历史数据
   *
   * @param symbol    交易对
   * @param period    {@link Period}
   * @param from      开始时间
   * @param to        结束时间
   * @param handler   异步结果处理器
   */
  public void reqKlineHistory(@NonNull String symbol, Period period, long from, long to,
                              @NonNull Handler<AsyncResult<List<KlineTick>>> handler) {
    kLineManager.pollTicks(symbol,period,from,to, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(ar.result()));
      }else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * 更新交易记录缓存
   *
   * @param data 交易记录
   */
  protected void updateTradeDetail (String symbol, List<TradeDetailTickData> data) {
    String ch = ChannelUtil.buildTradeDetailChannel(symbol);
    List<TradeDetailTickData> cache = tradeDetailHistory.computeIfAbsent(ch, k -> new ArrayList<>());
    List<TradeDetailTickData> temp = tradeDetailHistoryTemp.computeIfAbsent(ch, k -> new ArrayList<>());

    // cache始终保持30条，将需要新增的加进去
    cache.addAll(data);
    // 只保留30条历史数据
    if (cache.size() > 30) {
      temp.addAll(cache.subList(cache.size() - 30, cache.size()));
    }else {
      // 如果没超过30条，则拷贝到temp
      temp.addAll(cache);
    }
    // 交换cache 和 temp，交替使用
    tradeDetailHistory.put(ch,temp);
    cache.clear();
    tradeDetailHistoryTemp.put(ch,cache);
  }

  /**
   * 更新市场概要缓存
   *
   * @param data 市场概要
   */
  protected void updateMarketDetail (String symbol,MarketDetailTick data) {
    marketDetailCache.put(ChannelUtil.buildMarketDetailChannel(symbol),data);
  }

  /**
   * 更新市场深度缓存
   *
   * @param depthTick 深度
   */
  protected void updateMarketDepth (DepthTick depthTick) {

  }
}
