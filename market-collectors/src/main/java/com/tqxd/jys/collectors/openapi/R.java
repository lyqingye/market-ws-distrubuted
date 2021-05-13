package com.tqxd.jys.collectors.openapi;


import io.vertx.core.json.Json;

public class R<T> {
  private boolean success;
  private String errMsg;
  private T data;

  public static <T> String success(T data) {
    R<T> r = new R<>();
    r.success = true;
    r.data = data;
    return Json.encodePrettily(r);

  }

  public static <T> String success() {
    R<T> r = new R<>();
    r.success = true;
    return Json.encodePrettily(r);
  }

  public static <T> String fail(String errMsg) {
    R<T> r = new R<>();
    r.success = false;
    r.errMsg = errMsg;
    return Json.encodePrettily(r);
  }

  public static <T> String fail(Throwable throwable) {
    R<T> r = new R<>();
    r.success = false;
    r.errMsg = throwable.getMessage();
    return Json.encodePrettily(r);
  }

  public boolean getSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String getErrMsg() {
    return errMsg;
  }

  public void setErrMsg(String errMsg) {
    this.errMsg = errMsg;
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }
}
