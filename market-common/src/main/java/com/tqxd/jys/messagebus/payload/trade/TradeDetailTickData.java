package com.tqxd.jys.messagebus.payload.trade;

/**
 * https://huobiapi.github.io/docs/spot/v1/cn/#56c6c47284-2
 *
 * @author yjt
 * @since 2020/10/15 下午6:49
 */
public class TradeDetailTickData {
  private String id;
  private String tradeId;
  private String amount;

  private String price;

  private Long ts;

  private String direction;

  public String getTradeId() {
    return tradeId;
  }

  public void setTradeId(String tradeId) {
    this.tradeId = tradeId;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public TradeDetailTickData() {
  }

  public String getAmount() {
    return this.amount;
  }

  public void setAmount(String amount) {
    this.amount = amount;
  }

  public String getPrice() {
    return this.price;
  }

  public void setPrice(String price) {
    this.price = price;
  }

  public Long getTs() {
    return this.ts;
  }

  public void setTs(Long ts) {
    this.ts = ts;
  }

  public String getDirection() {
    return this.direction;
  }

  public void setDirection(String direction) {
    this.direction = direction;
  }

  public boolean equals(final Object o) {
    if (o == this) return true;
    if (!(o instanceof TradeDetailTickData)) return false;
    final TradeDetailTickData other = (TradeDetailTickData) o;
    if (!other.canEqual((Object) this)) return false;
    final Object this$amount = this.getAmount();
    final Object other$amount = other.getAmount();
    if (this$amount == null ? other$amount != null : !this$amount.equals(other$amount)) return false;
    final Object this$price = this.getPrice();
    final Object other$price = other.getPrice();
    if (this$price == null ? other$price != null : !this$price.equals(other$price)) return false;
    final Object this$ts = this.getTs();
    final Object other$ts = other.getTs();
    if (this$ts == null ? other$ts != null : !this$ts.equals(other$ts)) return false;
    final Object this$direction = this.getDirection();
    final Object other$direction = other.getDirection();
    if (this$direction == null ? other$direction != null : !this$direction.equals(other$direction)) return false;
    return true;
  }

  protected boolean canEqual(final Object other) {
    return other instanceof TradeDetailTickData;
  }

  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final Object $amount = this.getAmount();
    result = result * PRIME + ($amount == null ? 43 : $amount.hashCode());
    final Object $price = this.getPrice();
    result = result * PRIME + ($price == null ? 43 : $price.hashCode());
    final Object $ts = this.getTs();
    result = result * PRIME + ($ts == null ? 43 : $ts.hashCode());
    final Object $direction = this.getDirection();
    result = result * PRIME + ($direction == null ? 43 : $direction.hashCode());
    return result;
  }

  public String toString() {
    return "TradeDetailTickData(amount=" + this.getAmount() + ", price=" + this.getPrice() + ", ts=" + this.getTs() + ", direction=" + this.getDirection() + ")";
  }
}
