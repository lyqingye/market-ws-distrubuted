package com.tqxd.jys.spi.messagebus;


/**
 * 消息监听器
 *
 * @author lyqingye
 */
@FunctionalInterface
public interface MessageListener {

  /**
   * 收到消息事件
   *
   * @param message 消息
   */
  void onMessage(Message<?> message);
}
