package com.tqxd.jys.timeline.cmd;


import java.util.concurrent.CompletableFuture;

public class CmdResult<T> extends CompletableFuture<T> {
  private boolean success;

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }
}
