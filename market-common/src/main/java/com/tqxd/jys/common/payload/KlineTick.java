package com.tqxd.jys.common.payload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tqxd.jys.timeline.TimeLineShotData;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;

@DataObject
public class KlineTick implements TimeLineShotData {
  private Long id;

  private BigDecimal amount;

  private Integer count;

  private BigDecimal open;

  private BigDecimal close;

  private BigDecimal low;

  private BigDecimal high;

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
  @Override
  @JsonIgnore
  public long getTime() {
    return id * 1000;
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
  @Override
  public TimeLineShotData merge(TimeLineShotData target) {
    KlineTick tick = (KlineTick) target;

    this.count += tick.getCount();
    this.amount = this.amount.add(tick.getAmount());
    this.vol = this.vol.add(tick.getVol());
    this.close = tick.close;

    if (tick.high.compareTo(this.high) > 0) {
      this.high = tick.high;
    }
    if (tick.low.compareTo(this.low) < 0) {
      this.low = tick.low;
    }
    return this;
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
