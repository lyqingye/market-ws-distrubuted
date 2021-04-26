package com.tqxd.jys.disruptor;


/**
 * @author yjt
 * @since 2020/9/24 下午3:26
 */
public class ObjectEvent<T> {
  private T obj;

  public void clear() {
    this.obj = null;
  }

  public T getObj() {
    return obj;
  }

  public void setObj(T obj) {
    this.obj = obj;
  }
}
