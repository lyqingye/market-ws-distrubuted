package com.tqxd.jys.websocket.context;

import com.tqxd.jys.messagebus.payload.trade.TradeDetailTickData;
import com.tqxd.jys.timeline.KLine;
import com.tqxd.jys.websocket.session.SessionManager;
import com.tqxd.jys.websocket.session.WsSession;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yjt
 * @since 2020/11/14 18:13
 */
public class PushingContext {
  /**
   * 会话管理
   * <p>
   * channelId -> websocket session
   */
  private SessionManager<WsSession> sm = new SessionManager<>(4096);

  /**
   * 订阅K线了的会话
   */
  private Map<String, WsSession> klineSM = new ConcurrentHashMap<>(4096);

  /**
   * 订阅深度了的会话
   */
  private Map<String, WsSession> depthSM = new ConcurrentHashMap<>(4096);

  /**
   * 订阅市场详情了的会话
   */
  private Map<String, WsSession> detailSM = new ConcurrentHashMap<>(4096);

  /**
   * K线实时热点数据
   */
  private Map<String, KLine> klineTimeLineMap = new ConcurrentHashMap<>(64);

  /**
   * 市场详情缓存数据
   */
  private Map<String, Buffer> marketDetailCache = new ConcurrentHashMap<>();

  /**
   * 最后成交数据缓存
   */
  private Map<String, Buffer> latestTradeBufferCache = new ConcurrentHashMap<>();

  /**
   * 最后成交数据缓存
   */
  private Map<String, List<TradeDetailTickData>> latestTradeCache = new ConcurrentHashMap<>();

  /**
   * 盘口缓存
   */
  private Map<String, Buffer> depthChartCache = new ConcurrentHashMap<>();

  /**
   * vertx 实例
   */
  private Vertx vertx;

  /**
   * hide default constructor
   */
  private PushingContext() {
  }

  public void init(Vertx vertx) {
    this.vertx = Objects.requireNonNull(vertx);
  }
//    /**
//     * 更新市场缓存
//     *
//     * @param symbol 交易对
//     * @return 更新后的缓存
//     */
//    public Buffer updateMarketDetailTick(String symbol) {
//        MarketDetailTick detail = new MarketDetailTick(this.getOrCreateTimeWheel(symbol, Period._1_MIN).toArray());
//        // 真正的发送的市场详情对象
//        String detailSub = HuoBiUtils.toDetailSub(symbol);
//        MarketDetailResp resp = MarketDetailResp.of(detailSub, detail);
//        Buffer buffer = null;
//        try {
//            buffer = Buffer.buffer(GZIPUtils.compress(Json.encodeToBuffer(resp).getBytes()));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        this.marketDetailCache.put(detailSub, buffer);
//        return buffer;
//    }

  public SessionManager<WsSession> getSm() {
    return sm;
  }

  public void setSm(SessionManager<WsSession> sm) {
    this.sm = sm;
  }

  public Map<String, WsSession> getKlineSM() {
    return klineSM;
  }

  public void setKlineSM(Map<String, WsSession> klineSM) {
    this.klineSM = klineSM;
  }

  public Map<String, WsSession> getDepthSM() {
    return depthSM;
  }

  public void setDepthSM(Map<String, WsSession> depthSM) {
    this.depthSM = depthSM;
  }

  public Map<String, WsSession> getDetailSM() {
    return detailSM;
  }

  public void setDetailSM(Map<String, WsSession> detailSM) {
    this.detailSM = detailSM;
  }

  public Map<String, KLine> getKlineTimeLineMap() {
    return klineTimeLineMap;
  }

  public void setKlineTimeLineMap(Map<String, KLine> klineTimeLineMap) {
    this.klineTimeLineMap = klineTimeLineMap;
  }

  public Map<String, Buffer> getMarketDetailCache() {
    return marketDetailCache;
  }

  public void setMarketDetailCache(Map<String, Buffer> marketDetailCache) {
    this.marketDetailCache = marketDetailCache;
  }

  public Map<String, Buffer> getLatestTradeBufferCache() {
    return latestTradeBufferCache;
  }

  public void setLatestTradeBufferCache(Map<String, Buffer> latestTradeBufferCache) {
    this.latestTradeBufferCache = latestTradeBufferCache;
  }

  public Map<String, List<TradeDetailTickData>> getLatestTradeCache() {
    return latestTradeCache;
  }

  public void setLatestTradeCache(Map<String, List<TradeDetailTickData>> latestTradeCache) {
    this.latestTradeCache = latestTradeCache;
  }

  public Map<String, Buffer> getDepthChartCache() {
    return depthChartCache;
  }

  public void setDepthChartCache(Map<String, Buffer> depthChartCache) {
    this.depthChartCache = depthChartCache;
  }

  public Vertx getVertx() {
    return vertx;
  }

  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }
}
