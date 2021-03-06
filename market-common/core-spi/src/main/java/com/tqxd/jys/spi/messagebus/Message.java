package com.tqxd.jys.spi.messagebus;


/**
 * 封装消息
 */
public class Message<T> {
  /**
   * 消息索引
   */
  private long index;

  /**
   * 消息来源
   */
  private String from;

  /**
   * 消息类型
   */
  private DataType type;

  /**
   * 消息发送时间戳
   */
  private long ts;

  /**
   * 消息体
   */
  private T payload;

  public static <E> Message<E> withData(DataType type, String from, E payload) {
    Message<E> msg = new Message<>();
    msg.setType(type);
    msg.setFrom(from);
    msg.setPayload(payload);
    msg.setTs(System.currentTimeMillis());
    return msg;
  }


  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

  public T getPayload() {
    return payload;
  }

  public void setPayload(T payload) {
    this.payload = payload;
  }

  public DataType getType() {
    return type;
  }

  public void setType(DataType type) {
    this.type = type;
  }
}
