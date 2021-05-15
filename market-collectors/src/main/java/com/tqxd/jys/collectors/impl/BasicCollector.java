package com.tqxd.jys.collectors.impl;

import com.tqxd.jys.constance.DataType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BasicCollector extends AbstractVerticle implements Collector {
  private Logger log = LoggerFactory.getLogger(BasicCollector.class);
  private Map<DataType, List<String>> subscribed = new HashMap<>(16);
  private DataReceiver[] receivers = new DataReceiver[16];
  private int numOfReceives = 0;

  @Override
  public synchronized void restart(Handler<AsyncResult<Void>> handler) {
    stopFuture()
      .compose(none -> startFuture())
      .onComplete(handler);
  }

  @Override
  public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    if (!this.isRunning()) {
      handler.handle(Future.failedFuture("the collector is not running yet!"));
    } else {
      List<String> symbols;
      if ((symbols = subscribed.get(dataType)) != null) {
        for (String exist : symbols) {
          if (exist.equals(symbol)) {
            handler.handle(Future.succeededFuture());
            return;
          }
        }
      }
      subscribed.computeIfAbsent(dataType, k -> new ArrayList<>()).add(symbol);
      handler.handle(Future.succeededFuture());
    }
  }

  /**
   * 取消订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否取消订阅成功
   */
  @Override
  public void unSubscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    List<String> symbols = subscribed.get(dataType);
    if (symbols == null) {
      handler.handle(Future.succeededFuture());
    } else {
      Iterator<String> it = symbols.iterator();
      while (it.hasNext()) {
        String obj = it.next();
        if (obj.equals(symbol)) {
          it.remove();
          handler.handle(Future.succeededFuture());
          return;
        }
      }
      handler.handle(Future.failedFuture("not found the subscribed info for: " + dataType + ":" + symbol));
    }
  }

  /**
   * 获取当前正在订阅的交易对
   *
   * @return 当前正在订阅的信息, key为数据收集类型, value为交易对列表
   */
  @Override
  public Map<DataType, List<String>> listSubscribedInfo() {
    return subscribed;
  }

  @Override
  public synchronized void addDataReceiver(DataReceiver receiver) {
    if (numOfReceives >= receivers.length) {
      DataReceiver[] newReceives = new DataReceiver[receivers.length << 1];
      System.arraycopy(receivers, 0, newReceives, 0, numOfReceives);
      receivers = newReceives;
    }
    receivers[numOfReceives++] = receiver;
  }

  @Override
  public void unParkReceives(DataType dataType, JsonObject json) {
    for (int i = 0; i < numOfReceives; i++) {
      receivers[i].onReceive(this, dataType, json);
    }
  }
}
