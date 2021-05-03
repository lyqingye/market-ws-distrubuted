package com.tqxd.jys.timeline;

import com.tqxd.jys.timeline.cmd.AppendTickResult;
import com.tqxd.jys.timeline.cmd.AutoAggregateResult;

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
  default void onAppendFinished (AppendTickResult rs) {}

  /**
   * 自动聚合事件
   *
   * @param rs 结果
   */
  default void onAutoAggregate(AutoAggregateResult rs) {}
}
