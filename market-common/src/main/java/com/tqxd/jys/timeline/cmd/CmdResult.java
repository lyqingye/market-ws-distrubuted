package com.tqxd.jys.timeline.cmd;


import java.util.concurrent.CompletableFuture;

public class CmdResult<T> extends CompletableFuture<T> {
  private boolean success;
  private String reason;

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String getReason() {
    return this.reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }
}
