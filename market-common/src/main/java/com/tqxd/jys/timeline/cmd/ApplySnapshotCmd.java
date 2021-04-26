package com.tqxd.jys.timeline.cmd;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.Collections;
import java.util.List;

public class ApplySnapshotCmd {
  private String symbol;
  private Period period;
  private long commitIndex;
  private List<KlineTick> ticks = Collections.emptyList();
  private Handler<AsyncResult<Void>> handler;

  public Handler<AsyncResult<Void>> getHandler() {
    return handler;
  }

  public void setHandler(Handler<AsyncResult<Void>> handler) {
    this.handler = handler;
  }

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public Period getPeriod() {
    return period;
  }

  public void setPeriod(Period period) {
    this.period = period;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public List<KlineTick> getTicks() {
    return ticks;
  }

  public void setTicks(List<KlineTick> ticks) {
    this.ticks = ticks;
  }
}
