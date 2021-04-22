package com.tqxd.jys.messagebus.payload.detail;

/**
 * @author yjt
 * @since 2020/10/15 上午9:33
 */
public class MarketDetailResp {

  private final Long ts = System.currentTimeMillis();
  private String ch;
  private MarketDetailTick tick;

  public MarketDetailResp() {
  }

  public static MarketDetailResp of(String ch, MarketDetailTick tick) {
    MarketDetailResp resp = new MarketDetailResp();
    resp.setCh(ch);
    resp.setTick(tick);
    return resp;
  }

  public String getCh() {
    return this.ch;
  }

  public void setCh(String ch) {
    this.ch = ch;
  }

  public Long getTs() {
    return this.ts;
  }

  public MarketDetailTick getTick() {
    return this.tick;
  }

  public void setTick(MarketDetailTick tick) {
    this.tick = tick;
  }

  public boolean equals(final Object o) {
    if (o == this) return true;
    if (!(o instanceof MarketDetailResp)) return false;
    final MarketDetailResp other = (MarketDetailResp) o;
    if (!other.canEqual((Object) this)) return false;
    final Object this$ch = this.getCh();
    final Object other$ch = other.getCh();
    if (this$ch == null ? other$ch != null : !this$ch.equals(other$ch)) return false;
    final Object this$ts = this.getTs();
    final Object other$ts = other.getTs();
    if (this$ts == null ? other$ts != null : !this$ts.equals(other$ts)) return false;
    final Object this$tick = this.getTick();
    final Object other$tick = other.getTick();
    if (this$tick == null ? other$tick != null : !this$tick.equals(other$tick)) return false;
    return true;
  }

  protected boolean canEqual(final Object other) {
    return other instanceof MarketDetailResp;
  }

  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final Object $ch = this.getCh();
    result = result * PRIME + ($ch == null ? 43 : $ch.hashCode());
    final Object $ts = this.getTs();
    result = result * PRIME + ($ts == null ? 43 : $ts.hashCode());
    final Object $tick = this.getTick();
    result = result * PRIME + ($tick == null ? 43 : $tick.hashCode());
    return result;
  }

  public String toString() {
    return "MarketDetailResp(ch=" + this.getCh() + ", ts=" + this.getTs() + ", tick=" + this.getTick() + ")";
  }
}
