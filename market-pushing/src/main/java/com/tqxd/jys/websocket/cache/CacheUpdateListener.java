package com.tqxd.jys.websocket.cache;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.depth.DepthTick;
import com.tqxd.jys.messagebus.payload.trade.TradeDetailTick;

/**
 * 缓存监听者
 *
 * @author lyqingye
 */
public interface CacheUpdateListener {

  /**
   * k线数据变动事件
   *
   * @param symbol 交易对
   * @param period {@link Period}
   * @param tick tick
   */
  default void onKLineUpdate (String symbol, Period period, KlineTick tick){}

  /**
   * 成交记录变动事件
   *
   * @param symbol 交易对
   * @param tick tick
   */
  default void onTradeDetailUpdate (String symbol , TradeDetailTick tick) {}

  /**
   * 市场深度变动事件
   *
   * @param symbol 交易对
   * @param level {@link DepthLevel}
   * @param tick tick
   */
  default void onDepthUpdate (String symbol, DepthLevel level, DepthTick tick) {}

  /**
   * 市场概括变动事件
   *
   * @param symbol 交易对
   * @param tick   tick
   */
  default void onMarketDetailUpdate(String symbol, KlineTick tick) {
  }
}
