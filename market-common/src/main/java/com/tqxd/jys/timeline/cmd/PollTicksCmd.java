package com.tqxd.jys.timeline.cmd;


import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

public class PollTicksCmd {
  private String symbol;
  private Period period;
  private long from, to;
  private Handler<AsyncResult<List<KlineTick>>> handler;

  public Handler<AsyncResult<List<KlineTick>>> getHandler() {
    return handler;
  }

  public void setHandler(Handler<AsyncResult<List<KlineTick>>> handler) {
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

  public long getFrom() {
    return from;
  }

  public void setFrom(long from) {
    this.from = from;
  }

  public long getTo() {
    return to;
  }

  public void setTo(long to) {
    this.to = to;
  }
}
