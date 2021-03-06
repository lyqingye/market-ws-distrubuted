package com.tqxd.jys.websocket.cache;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.MessageListener;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.payload.depth.DepthTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTickData;
import com.tqxd.jys.timeline.KLineMeta;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.KLineRepositoryListener;
import com.tqxd.jys.timeline.cmd.AppendTickResult;
import com.tqxd.jys.timeline.cmd.Auto24HourStatisticsResult;
import com.tqxd.jys.utils.ChannelUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.*;

/**
 * 缓存管理器, 直接面向用户数据
 *
 * @author lyqingye
 */
public class CacheManager implements KLineRepositoryListener,MessageListener {
  /**
   * 市场概括缓存
   */
  private Map<String, KlineTick> marketDetailCache = new HashMap<>();

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
  private KLineRepository kLineRepository;
  private int numOfListener = 0;
  private static CacheDataWatcher[] WATCHERS = new CacheDataWatcher[255];

  public CacheManager(KLineRepository kLineRepository) {
    this.kLineRepository = Objects.requireNonNull(kLineRepository);
    this.kLineRepository.addListener(this);
  }

  public synchronized void addWatcher(CacheDataWatcher watcher) {
    if (numOfListener >= WATCHERS.length) {
      CacheDataWatcher[] newListeners = new CacheDataWatcher[numOfListener << 1];
      System.arraycopy(WATCHERS, 0, newListeners, 0, numOfListener);
      WATCHERS = newListeners;
    }
    WATCHERS[numOfListener++] = Objects.requireNonNull(watcher);
  }

  /**
   * 查询全量深度
   *
   * @param symbol 交易对
   * @param level  深度等级 {@link DepthLevel}
   * @return null or 深度数据
   */
  public DepthTick reqDepth(String symbol, DepthLevel level, int size) {
    return marketDepthData.get(symbol + ":" + level);
  }

  /**
   * 查询全量成交明细
   *
   * @param symbol 交易对
   * @param size   条数，默认30
   * @return null or 成交明细数据
   */
  public List<TradeDetailTickData> reqTradeDetail(String symbol, int size) {
    List<TradeDetailTickData> data = tradeDetailHistory.get(ChannelUtil.buildTradeDetailChannel(symbol));
    if (data != null) {
      List<TradeDetailTickData> subList;
      if (size > data.size()) {
        subList = data;
      } else {
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
  public KlineTick reqMarketDetail(String symbol) {
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
  public void reqTimeSharing(String symbol, long from, long to,
                             Handler<AsyncResult<Buffer>> handler) {
    // TODO 暂不支持
  }

  /**
   * 查询k线历史数据
   *
   * @param symbol  交易对
   * @param period  {@link Period}
   * @param from    开始时间
   * @param to      结束时间
   * @param handler 异步结果处理器
   */
  public void reqKlineHistory(String symbol, Period period, long from, long to,
                              Handler<AsyncResult<List<KlineTick>>> handler) {
    kLineRepository.query(symbol, period, from, to, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(ar.result()));
      } else {
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
  protected void updateMarketDetail(String symbol, KlineTick data) {
    marketDetailCache.put(ChannelUtil.buildMarketDetailChannel(symbol), data);
  }

  /**
   * 更新市场深度缓存
   *
   * @param symbol 交易对
   * @param level {@link DepthLevel}
   * @param depthTick 深度
   */
  protected void updateMarketDepth (String symbol,DepthLevel level, DepthTick depthTick) {
    marketDepthData.put(symbol + ":" + level, depthTick);
  }

  //
  // 监听k线仓库变动
  //

  @Override
  public void onAppendFinished(AppendTickResult rs) {
    KLineMeta meta = rs.getMeta();
    for (int i = 0; i < numOfListener; i++) {
      WATCHERS[i].onKLineUpdate(meta.getSymbol(), meta.getPeriod(), rs.getTick());
    }
    if (rs.getDetail() != null) {
      updateMarketDetail(meta.getSymbol(), rs.getDetail());
      for (int i = 0; i < numOfListener; i++) {
        WATCHERS[i].onMarketDetailUpdate(meta.getSymbol(), rs.getDetail());
      }
    }
  }

  @Override
  public void onAutoAggregate(Auto24HourStatisticsResult aggregate) {
    KLineMeta meta = aggregate.getMeta();
    updateMarketDetail(aggregate.getMeta().getSymbol(), aggregate.getTick());
    for (int i = 0; i < numOfListener; i++) {
      WATCHERS[i].onMarketDetailUpdate(meta.getSymbol(), aggregate.getTick());
    }
  }

  //
  // 监听消息队列消息
  //

  @Override
  public void onMessage(Message<?> message) {
    JsonObject json = (JsonObject) Json.decodeValue((String) message.getPayload());
    String ch = json.getString("ch");
    String symbol = ChannelUtil.getSymbol(ch);
    JsonObject tick = json.getJsonObject("tick");
    if (tick == null) {
      return;
    }
    if (DataType.TRADE_DETAIL.equals(message.getType())) {
      TradeDetailTick tradeDetail = tick.mapTo(TradeDetailTick.class);
      updateTradeDetail(symbol, tradeDetail.getData());
      for (int i = 0; i < numOfListener; i++) {
        WATCHERS[i].onTradeDetailUpdate(symbol, tradeDetail);
      }
    } else if (DataType.DEPTH.equals(message.getType())) {
      DepthLevel depthLevel = ChannelUtil.getDepthLevel(ch);
      DepthTick depthTick = tick.mapTo(DepthTick.class);
      updateMarketDepth(symbol, depthLevel, depthTick);
      for (int i = 0; i < numOfListener; i++) {
        WATCHERS[i].onDepthUpdate(symbol, depthLevel, depthTick);
      }
    }
  }
}
