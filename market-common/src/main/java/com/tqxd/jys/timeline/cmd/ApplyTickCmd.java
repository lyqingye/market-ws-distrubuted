package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public class ApplyTickCmd {
  private String symbol;
  private Period period;
  private long commitIndex;
  private KlineTick tick;
  private Handler<AsyncResult<Long>> handler;

  public Handler<AsyncResult<Long>> getHandler() {
    return handler;
  }

  public void setHandler(Handler<AsyncResult<Long>> handler) {
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

  public KlineTick getTick() {
    return tick;
  }

  public void setTick(KlineTick tick) {
    this.tick = tick;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }
}
