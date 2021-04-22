package com.tqxd.jys.common.payload;

/**
 * 火币统一格式返回值
 *
 * @param <T>
 */
public class TemplatePayload<T> {
  /**
   * channel 主题
   */
  private String ch;
  /**
   * 时间戳
   */
  private long ts = System.currentTimeMillis() / 1000;
  private T tick;

  public static <E> TemplatePayload<E> of(String ch, E tick) {
    TemplatePayload<E> rs = new TemplatePayload<>();
    rs.setCh(ch);
    rs.setTick(tick);
    return rs;
  }

  public String getCh() {
    return ch;
  }

  public void setCh(String ch) {
    this.ch = ch;
  }

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

  public T getTick() {
    return tick;
  }

  public void setTick(T tick) {
    this.tick = tick;
  }
}
