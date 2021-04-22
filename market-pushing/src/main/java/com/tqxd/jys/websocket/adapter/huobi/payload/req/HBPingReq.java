package com.tqxd.jys.websocket.adapter.huobi.payload.req;

/**
 * @author yjt
 * @since 2020/9/28 上午9:18
 */
public class HBPingReq {
  private long ping;

  public static HBPingReq ping() {
    HBPingReq ping = new HBPingReq();
    ping.setPing(System.currentTimeMillis());
    return ping;
  }

  public long getPing() {
    return ping;
  }

  public void setPing(long ping) {
    this.ping = ping;
  }
}
