package com.tqxd.jys.websocket.processor;

import com.tqxd.jys.timeline.KLineManager;
import com.tqxd.jys.websocket.CacheManager;
import com.tqxd.jys.websocket.session.SessionManager;

import java.util.Objects;

/**
 * 处理器上下文, 存放推送时所需要到的数据
 */
public class Context {
  /**
   * 会话管理器
   */
  private SessionManager sessionManager;

  /**
   * k线管理器
   */
  private KLineManager klineManager;

  /**
   *
   */
  private CacheManager cacheManager;

  public Context(SessionManager sessionMgr, KLineManager klineManager, CacheManager cacheManager) {
    this.sessionManager = Objects.requireNonNull(sessionMgr);
    this.klineManager = Objects.requireNonNull(klineManager);
    this.cacheManager = Objects.requireNonNull(cacheManager);
  }

  public SessionManager sessionManager() {
    return sessionManager;
  }

  public KLineManager klineManager() {
    return klineManager;
  }

  public CacheManager cacheManager() {
    return cacheManager;
  }

}
