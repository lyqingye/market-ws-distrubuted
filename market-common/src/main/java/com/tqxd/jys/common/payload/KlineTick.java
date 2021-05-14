package com.tqxd.jys.common.payload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@DataObject
public class KlineTick{
  /**
   *unix时间，同时作为K线ID
   */
  private Long id;

  /**
   *成交量
   */
  private BigDecimal amount;

  /**
   *成交笔数
   */
  private Integer count;

  /**
   *开盘价
   */
  private BigDecimal open;

  /**
   *收盘价（当K线为最晚的一根时，是最新成交价）
   */
  private BigDecimal close;

  /**
   *最低价
   */
  private BigDecimal low;

  /**
   *最高价
   */
  private BigDecimal high;


  /**
   * 成交额, 即 sum(每一笔成交价 * 该笔的成交量)
   */
  private BigDecimal vol;

  public KlineTick() {
  }

  public KlineTick(JsonObject json) {
    final KlineTick tick = json.mapTo(KlineTick.class);
    this.id = tick.id;
    this.open = tick.open;
    this.amount = tick.amount;
    this.count = tick.count;
    this.close = tick.close;
    this.low = tick.low;
    this.high = tick.high;
    this.vol = tick.vol;
  }

  /**
   * 当前当前数据槽对应的时间
   *
   * @return 单位 mill
   */
  @JsonIgnore
  public long getTime() {
    return id + TimeUnit.HOURS.toSeconds(8);
  }


  @Override
  public KlineTick clone() {
    KlineTick tick = new KlineTick();
    tick.id = this.id;
    tick.amount = this.amount;
    tick.vol = this.vol;
    tick.high = this.high;
    tick.low = this.low;
    tick.open = this.open;
    tick.close = this.close;
    tick.count = this.count;
    return tick;
  }

  /**
   * 合并两个数据槽的数据
   *
   * @param target 目标
   * @return 合并后的数据
   */
  public KlineTick merge(KlineTick target) {
    this.count = target.getCount();
    this.amount = target.getAmount();
    this.vol = target.getVol();
    this.close = target.close;

    if (target.high.compareTo(this.high) > 0) {
      this.high = target.high;
    }
    if (target.low.compareTo(this.low) < 0) {
      this.low = target.low;
    }
    return this;
  }

  public KlineTick sum(KlineTick target) {
    this.id = target.id;
    this.count += target.getCount();
    this.amount = this.amount.add(target.getAmount());
    this.vol = this.vol.add(target.getVol());
    this.close = target.getClose();

    if (target.high.compareTo(this.high) > 0) {
      this.high = target.high;
    }
    if (target.low.compareTo(this.low) < 0) {
      this.low = target.low;
    }
    return this;
  }

  public KlineTick deepClone() {
    KlineTick tick = new KlineTick();
    tick.id = this.id;
    tick.open = BigDecimal.valueOf(this.open.doubleValue());
    tick.close = BigDecimal.valueOf(this.close.doubleValue());
    tick.high = BigDecimal.valueOf(this.high.doubleValue());
    tick.low = BigDecimal.valueOf(this.low.doubleValue());
    tick.vol = BigDecimal.valueOf(this.vol.doubleValue());
    tick.amount = BigDecimal.valueOf(this.amount.doubleValue());
    tick.count = this.count;
    return tick;
  }

  public JsonObject toJson() {
    return JsonObject.mapFrom(this);
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public BigDecimal getOpen() {
    return open;
  }

  public void setOpen(BigDecimal open) {
    this.open = open;
  }

  public BigDecimal getClose() {
    return close;
  }

  public void setClose(BigDecimal close) {
    this.close = close;
  }

  public BigDecimal getLow() {
    return low;
  }

  public void setLow(BigDecimal low) {
    this.low = low;
  }

  public BigDecimal getHigh() {
    return high;
  }

  public void setHigh(BigDecimal high) {
    this.high = high;
  }

  public BigDecimal getVol() {
    return vol;
  }

  public void setVol(BigDecimal vol) {
    this.vol = vol;
  }
}
