package com.tqxd.jys.core.kline;

import com.tqxd.jys.core.kline.cmd.AppendTickResult;
import com.tqxd.jys.core.kline.cmd.AutoAggregateResult;

/**
 * k线仓库事件监听
 *
 * @author lyqingye
 */
public interface KLineRepositoryListener {
  /**
   * 附加k线tick结束事件
   *
   * @param rs 结果
   */
  default void onAppendFinished(AppendTickResult rs) {
  }

  /**
   * 自动聚合事件
   *
   * @param rs 结果
   */
  default void onAutoAggregate(AutoAggregateResult rs) {
  }
}
