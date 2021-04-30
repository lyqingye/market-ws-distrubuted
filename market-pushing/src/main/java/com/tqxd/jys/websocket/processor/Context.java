package com.tqxd.jys.websocket.processor;

import com.tqxd.jys.timeline.KLineManager;
import com.tqxd.jys.websocket.session.FastSessionMgr;

/**
 * 处理器上下文, 存放推送时所需要到的数据
 */
public class Context {
  /**
   * 会话管理器
   */
  private FastSessionMgr sessionMgr;

  /**
   * k线管理器
   */
  private KLineManager klineManager;

  public Context(FastSessionMgr sessionMgr, KLineManager klineManager) {
    this.sessionMgr = sessionMgr;
    this.klineManager = klineManager;
  }

  public FastSessionMgr getSessionMgr() {
    return sessionMgr;
  }

  public KLineManager getKlineManager() {
    return klineManager;
  }
}
