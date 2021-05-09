package com.tqxd.jys.core.message.detail;

import java.math.BigDecimal;

/**
 * @author yjt
 * @since 2020/10/14 下午7:53
 */
public class MarketDetailTick {
  /**
   * unix 时间
   */
  private final Integer id = Math.toIntExact(System.currentTimeMillis() / 1000);

  /**
   * 24小时成交量
   */
  private BigDecimal amount = BigDecimal.ZERO;

  /**
   * 24小时成交笔数
   */
  private Integer count = 0;

  /**
   * 开盘价
   */
  private BigDecimal open;

  /**
   * 收盘价
   */
  private BigDecimal close = BigDecimal.ZERO;

  /**
   * 最高价
   */
  private BigDecimal high = BigDecimal.ZERO;

  /**
   * 最低价
   */
  private BigDecimal low = BigDecimal.ZERO;

  /**
   * 成交额
   */
  private BigDecimal vol = BigDecimal.ZERO;

  public MarketDetailTick() {
  }

  public Integer getId() {
    return this.id;
  }

  public BigDecimal getAmount() {
    return this.amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public Integer getCount() {
    return this.count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public BigDecimal getOpen() {
    return this.open;
  }

  public void setOpen(BigDecimal open) {
    this.open = open;
  }

  public BigDecimal getClose() {
    return this.close;
  }

  public void setClose(BigDecimal close) {
    this.close = close;
  }

  public BigDecimal getHigh() {
    return this.high;
  }

  public void setHigh(BigDecimal high) {
    this.high = high;
  }

  public BigDecimal getLow() {
    return this.low;
  }

  public void setLow(BigDecimal low) {
    this.low = low;
  }

  public BigDecimal getVol() {
    return this.vol;
  }

  public void setVol(BigDecimal vol) {
    this.vol = vol;
  }

  public boolean equals(final Object o) {
    if (o == this) return true;
    if (!(o instanceof MarketDetailTick)) return false;
    final MarketDetailTick other = (MarketDetailTick) o;
    if (!other.canEqual((Object) this)) return false;
    final Object this$id = this.getId();
    final Object other$id = other.getId();
    if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
    final Object this$amount = this.getAmount();
    final Object other$amount = other.getAmount();
    if (this$amount == null ? other$amount != null : !this$amount.equals(other$amount)) return false;
    final Object this$count = this.getCount();
    final Object other$count = other.getCount();
    if (this$count == null ? other$count != null : !this$count.equals(other$count)) return false;
    final Object this$open = this.getOpen();
    final Object other$open = other.getOpen();
    if (this$open == null ? other$open != null : !this$open.equals(other$open)) return false;
    final Object this$close = this.getClose();
    final Object other$close = other.getClose();
    if (this$close == null ? other$close != null : !this$close.equals(other$close)) return false;
    final Object this$high = this.getHigh();
    final Object other$high = other.getHigh();
    if (this$high == null ? other$high != null : !this$high.equals(other$high)) return false;
    final Object this$low = this.getLow();
    final Object other$low = other.getLow();
    if (this$low == null ? other$low != null : !this$low.equals(other$low)) return false;
    final Object this$vol = this.getVol();
    final Object other$vol = other.getVol();
    if (this$vol == null ? other$vol != null : !this$vol.equals(other$vol)) return false;
    return true;
  }

  protected boolean canEqual(final Object other) {
    return other instanceof MarketDetailTick;
  }

  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final Object $id = this.getId();
    result = result * PRIME + ($id == null ? 43 : $id.hashCode());
    final Object $amount = this.getAmount();
    result = result * PRIME + ($amount == null ? 43 : $amount.hashCode());
    final Object $count = this.getCount();
    result = result * PRIME + ($count == null ? 43 : $count.hashCode());
    final Object $open = this.getOpen();
    result = result * PRIME + ($open == null ? 43 : $open.hashCode());
    final Object $close = this.getClose();
    result = result * PRIME + ($close == null ? 43 : $close.hashCode());
    final Object $high = this.getHigh();
    result = result * PRIME + ($high == null ? 43 : $high.hashCode());
    final Object $low = this.getLow();
    result = result * PRIME + ($low == null ? 43 : $low.hashCode());
    final Object $vol = this.getVol();
    result = result * PRIME + ($vol == null ? 43 : $vol.hashCode());
    return result;
  }

  public String toString() {
    return "MarketDetailTick(id=" + this.getId() + ", amount=" + this.getAmount() + ", count=" + this.getCount() + ", open=" + this.getOpen() + ", close=" + this.getClose() + ", high=" + this.getHigh() + ", low=" + this.getLow() + ", vol=" + this.getVol() + ")";
  }
}
