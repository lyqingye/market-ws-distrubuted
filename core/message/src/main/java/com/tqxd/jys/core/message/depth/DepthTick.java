package com.tqxd.jys.core.message.depth;


/**
 * @author yjt
 * @since 2020/10/12 上午9:18
 */
public class DepthTick {

  private static final String[][] EMPTY = new String[0][0];

  private String[][] bids = EMPTY;

  private String[][] asks = EMPTY;

  private String version;

  private Long ts;

  public static String[][] getEMPTY() {
    return EMPTY;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String[][] getBids() {
    return bids;
  }

  public void setBids(String[][] bids) {
    this.bids = bids;
  }

  public String[][] getAsks() {
    return asks;
  }

  public void setAsks(String[][] asks) {
    this.asks = asks;
  }

  public Long getTs() {
    return ts;
  }

  public void setTs(Long ts) {
    this.ts = ts;
  }
}
