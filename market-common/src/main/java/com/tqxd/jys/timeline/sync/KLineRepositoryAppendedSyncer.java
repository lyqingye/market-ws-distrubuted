package com.tqxd.jys.timeline.sync;

import com.tqxd.jys.timeline.KLineRepository;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

public interface KLineRepositoryAppendedSyncer {

  /**
   * 同步到指定仓库
   *
   * @param target  目标仓库
   * @param handler 结果处理器
   */
  void syncAppendedTo(KLineRepository target, Handler<AsyncResult<Void>> handler);

  default Future<Void> syncAppendTo(KLineRepository target) {
    Promise<Void> promise = Promise.promise();
    syncAppendedTo(target, promise);
    return promise.future();
  }
}
