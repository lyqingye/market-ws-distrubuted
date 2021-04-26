package com.tqxd.jys.websocket.cache;

import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * 缓存管理器, 直接面向用户数据
 *
 * @author lyqingye
 */
public class CacheManager {
  /**
   * 查询全量深度
   *
   * @param symbol 交易对
   * @param level  深度等级 {@link DepthLevel}
   * @return null or 深度数据
   */
  public @Nullable Buffer reqDepth(@NonNull String symbol, @NonNull DepthLevel level) {
    return null;
  }

  /**
   * 查询全量成交明细
   *
   * @param symbol 交易对
   * @param size   条数，默认30
   * @return null or 成交明细数据
   */
  public @Nullable Buffer reqTradeDetail(@NonNull String symbol, int size) {
    if (size <= 0) {
      size = 30;
    }
    return null;
  }

  /**
   * 查询24小时市场概要
   *
   * @param symbol 交易对
   * @return null or 市场概要数据
   */
  public @Nullable Buffer reqMarketDetail(@NonNull String symbol) {
    return null;
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
                              @NonNull Handler<AsyncResult<Buffer>> handler) {
  }
}
