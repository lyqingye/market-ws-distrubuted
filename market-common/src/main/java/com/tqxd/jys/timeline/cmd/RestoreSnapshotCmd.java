package com.tqxd.jys.timeline.cmd;

import com.tqxd.jys.openapi.payload.KlineSnapshot;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public class RestoreSnapshotCmd {
  private KlineSnapshot snapshot;
  private Handler<AsyncResult<Void>> handler;

  public Handler<AsyncResult<Void>> getHandler() {
    return handler;
  }

  public void setHandler(Handler<AsyncResult<Void>> handler) {
    this.handler = handler;
  }

  public KlineSnapshot getSnapshot() {
    return snapshot;
  }

  public void setSnapshot(KlineSnapshot snapshot) {
    this.snapshot = snapshot;
  }
}
