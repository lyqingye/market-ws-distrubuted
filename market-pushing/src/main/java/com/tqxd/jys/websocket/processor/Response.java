package com.tqxd.jys.websocket.processor;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 响应体 (忽略null字段)
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Response<T> {
  private static final String ERR = "err";
  private static final String OK = "ok";
  private String id;
  private String status = OK;
  private String errMsg;
  private long ts = System.currentTimeMillis();
  private String rep;
  private String subbed;
  private String unsubbed;
  private T data;

  /**
   * example:
   * {
   * "id": "id1",
   * "status": "ok",
   * "subbed": "market.btcusdt.kline.1min",
   * "ts": 1489474081631
   * }
   */
  public static <T> Response<T> subOK(String id, String subbed) {
    Response<T> resp = new Response<>();
    resp.id = id;
    resp.subbed = subbed;
    return resp;
  }

  /**
   * example:
   * {
   * "id": "id1",
   * "status": "ok",
   * "unsubbed": "market.btcusdt.kline.1min",
   * "ts": 1489474081631
   * }
   */
  public static <T> Response<T> unSubOK(String id, String unsubbed) {
    Response<T> resp = new Response<>();
    resp.id = id;
    resp.unsubbed = unsubbed;
    return resp;
  }

  public static <T> Response<T> kLineReqOk(String id, String req, T data) {
    Response<T> resp = new Response<>();
    resp.id = id;
    resp.rep = req;
    resp.data = data;
    return resp;
  }

  public static <T> Response<T> err(String id, String req, String errMsg) {
    Response<T> resp = new Response<>();
    resp.status = id;
    resp.rep = req;
    resp.status = ERR;
    resp.errMsg = errMsg;
    return resp;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getErrMsg() {
    return errMsg;
  }

  public void setErrMsg(String errMsg) {
    this.errMsg = errMsg;
  }

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

  public String getRep() {
    return rep;
  }

  public void setRep(String rep) {
    this.rep = rep;
  }

  public String getSubbed() {
    return subbed;
  }

  public void setSubbed(String subbed) {
    this.subbed = subbed;
  }

  public String getUnsubbed() {
    return unsubbed;
  }

  public void setUnsubbed(String unsubbed) {
    this.unsubbed = unsubbed;
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }
}
