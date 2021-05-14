package com.tqxd.jys.collectors.impl.tqxd;


import com.tqxd.jys.collectors.impl.Collector;
import com.tqxd.jys.collectors.impl.DataReceiver;
import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.bian.BiAnDataConvert;
import com.tqxd.jys.collectors.impl.tqxd.match.TqxdMatchDepthCollector;
import com.tqxd.jys.collectors.impl.tqxd.match.TqxdMatchKlineCollector;
import com.tqxd.jys.collectors.impl.tqxd.match.TqxdMatchTradeCollector;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.utils.*;
import io.vertx.core.*;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author ex
 */
@SuppressWarnings("Duplicates")
public abstract class TqxdBaseCollector extends GenericWsCollector {
  private static final Logger log = LoggerFactory.getLogger(TqxdBaseCollector.class);
  public static final String SYMBOL_CONFIG = "symbol";
  public static final String INTERVAL_CONFIG = "interval";
  /**
   * 用于存储交易对的映射
   * 火币交易对映射 -> 用户自定义交易对映射
   */
  private Map<String, String> symbolDeMapping = new HashMap<>();

  /**
   * 订阅ID
   */
  private String subIdPrefix;

  /**
   * 由于深度订阅方式不一样，新建了Verticle，用于管理Verticle的创建和销毁
   */
  private Map<String,String> verticleMap = new HashMap<>();

  /**
   * 开启收集数据
   */
  @Override
  @SuppressWarnings("rawtypes")
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    HttpClientOptions httpClientOptions = new HttpClientOptions().setDefaultHost(config().getString("host"));
    config().put(HTTP_CLIENT_OPTIONS_PARAM, httpClientOptions);
    config().put(WS_REQUEST_PATH_PARAM, config().getString("path"));
    config().put(IDLE_TIME_OUT, 5000);
    subIdPrefix = UUID.randomUUID().toString();
    Promise<Void> promise = Promise.promise();
    super.start(promise);
    promise.future()
        .compose(none -> {
          List<Future> futures = new ArrayList<>();
          super.listSubscribedInfo().forEach(((collectDataType, symbols) -> {
            if (symbols != null) {
              for (String symbol : symbols) {
                futures.add(this.subscribe(collectDataType, symbol));
              }
            }
          }));
          if (futures.isEmpty()) {
            return Future.succeededFuture();
          }
          return CompositeFuture.any(futures);
        })
        .onSuccess(ar -> {
          log.info("[HuoBi]: start success!");
          startPromise.complete();
        })
        .onFailure(startPromise::fail);
  }


  /**
   * 订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否订阅成功
   */
  @Override
  public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future()
        .onSuccess(none -> {
          String id = subIdPrefix + symbol;
          JsonObject json = new JsonObject();
          json.put("id", id);
          switch (dataType) {
            case KLINE: {
              subscribeKline(symbol,handler);
              break;
            }
            case DEPTH: {
              subscribeTradeDetail(symbol,handler);
              break;
            }
            case TRADE_DETAIL: {
              subscribeDepth(symbol,handler);
              break;
            }
          }
          handler.handle(Future.succeededFuture());
        })
        .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  private void subscribeTradeDetail(String symbol,Handler<AsyncResult<Void>> handler){
    JsonObject cfg = this.getCfg(symbol);
    TqxdMatchTradeCollector tradeCollector = new TqxdMatchTradeCollector(cfg);
    TqxdBaseCollector that = this;

    tradeCollector.addDataReceiver(new DataReceiver() {
      @Override
      public void onReceive(Collector from, DataType dataType, JsonObject obj) {
        that.unParkReceives(dataType,TqxdDataConvert.trade(obj,symbol));
      }
    });

    vertx.deployVerticle(tradeCollector,ar->{
      if(ar.succeeded()){
        verticleMap.put(DataType.TRADE_DETAIL+symbol,ar.result());
        handler.handle(Future.succeededFuture());
      }else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });

  }


  private void subscribeKline(String symbol,Handler<AsyncResult<Void>> handler){
    JsonObject cfg = this.getCfg(symbol);
    TqxdMatchKlineCollector klineCollector = new TqxdMatchKlineCollector(cfg);
    TqxdBaseCollector that = this;

    klineCollector.addDataReceiver(new DataReceiver() {
      @Override
      public void onReceive(Collector from, DataType dataType, JsonObject obj) {
        // 将此事件传递给 BiAnKlineCollector
        // 这个事件最终会传递给 com.tqxd.jys.collectors.openapi.CollectorOpenApiImpl 做数据汇总处理
        that.unParkReceives(dataType, TqxdDataConvert.kline(obj, symbol));
      }
    });

    vertx.deployVerticle(klineCollector,ar->{
      if(ar.succeeded()){
        // 将此事件传递给 TqxdMatchKlineCollector
        // 这个事件最终会传递给 com.tqxd.jys.collectors.openapi.CollectorOpenApiImpl 做数据汇总处理
        verticleMap.put(DataType.KLINE+symbol,ar.result());
        handler.handle(Future.succeededFuture());
      }else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });

  }


  private void subscribeDepth(String symbol, Handler<AsyncResult<Void>> handler) {
    JsonObject cfg = this.getCfg(symbol);
    TqxdMatchDepthCollector depthCollector = new TqxdMatchDepthCollector(cfg);
    TqxdBaseCollector that = this;

    depthCollector.addDataReceiver(new DataReceiver() {
      @Override
      public void onReceive(Collector from, DataType dataType, JsonObject obj) {
        // 将此事件传递给 TqxdMatchDepthCollector
        // 这个事件最终会传递给 com.tqxd.jys.collectors.openapi.CollectorOpenApiImpl 做数据汇总处理
        that.unParkReceives(dataType, TqxdDataConvert.depth(obj, symbol));
      }
    });

    vertx.deployVerticle(depthCollector,ar -> {
      if (ar.succeeded()) {
        // 保存部署id
        verticleMap.put(DataType.DEPTH + symbol,ar.result());
        handler.handle(Future.succeededFuture());
      }else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private JsonObject getCfg(String symbol) {
    return new JsonObject()
            .put(HTTP_CLIENT_OPTIONS_PARAM, config().getValue(HTTP_CLIENT_OPTIONS_PARAM))
            .put(WS_REQUEST_PATH_PARAM, config().getValue(WS_REQUEST_PATH_PARAM))
            .put(SYMBOL_CONFIG, symbol)
            .put(IDLE_TIME_OUT, 5000);
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
    Promise<Void> promise = Promise.promise();
    super.unSubscribe(dataType, symbol, promise);
    promise.future()
        .onSuccess(none -> {
          String id = subIdPrefix + symbol;
          String unsub;
          JsonObject json = new JsonObject();
          json.put("id", id);
          switch (dataType) {
            case KLINE: {
              // 只订阅 1min的交易
              for (Period period : Period.values()) {
                unsub = HuoBiUtils.toKlineSub(toGenericSymbol(symbol), period);
                symbolDeMapping.put(unsub, HuoBiUtils.toKlineSub(symbol, period));
                json.put("unsub", unsub);
                log.info("[HuoBi]: unsubscribe: {}", unsub);
                super.writeText(json.toString());
              }
              break;
            }
            case DEPTH: {
              // 只订阅深度为0的
              unsub = HuoBiUtils.toDepthSub(toGenericSymbol(symbol), DepthLevel.step0);
              symbolDeMapping.put(unsub, HuoBiUtils.toDepthSub(symbol, DepthLevel.step0));
              json.put("unsub", unsub);
              log.info("[HuoBi]: unsubscribe: {}", unsub);
              super.writeText(json.toString());
              break;
            }
            case TRADE_DETAIL: {
              unsub = HuoBiUtils.toTradeDetailSub(toGenericSymbol(symbol));
              symbolDeMapping.put(unsub, HuoBiUtils.toTradeDetailSub(symbol));
              json.put("unsub", unsub);
              log.info("[HuoBi]: unsubscribe: {}", unsub);
              super.writeText(json.toString());
              break;
            }
          }
          handler.handle(Future.succeededFuture());
        })
        .onFailure(throwable -> {
          handler.handle(Future.failedFuture(throwable));
        });
  }

  @Override
  public String name() {
    return TqxdBaseCollector.class.getName();
  }

  /**
   * 描述一个收集器
   *
   * @return 收集器描述
   */
  @Override
  public String desc() {
    return "火币数据收集器";
  }

//  @Override
//  public void onFrame(WebSocket client, WebSocketFrame frame) {
//    super.onFrame(client, frame);
//    if (frame.isBinary() && frame.isFinal()) {
//      try {
//        byte[] data = GZIPUtils.fastDecompress(frame.binaryData());
//        JsonObject obj = (JsonObject) Json.decodeValue(new String(data, StandardCharsets.UTF_8));
//        // 如果是 ping 消息则需要回复 pong
//        if (isPingMsg(obj)) {
//          this.pong();
//        } else {
//          String ch = obj.getString("ch");
//          // 取消交易对映射
//          obj.put("ch", symbolDeMapping.get(ch));
//          // k线主题
//          if (ChannelUtil.isKLineChannel(ch)) {
//            unParkReceives(DataType.KLINE, obj);
//          } else if (ChannelUtil.isDepthChannel(ch)) {
//            unParkReceives(DataType.DEPTH, obj);
//          } else if (ChannelUtil.isTradeDetailChannel(ch)) {
//            unParkReceives(DataType.TRADE_DETAIL, obj);
//          }
//        }
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//    } else if (frame.isPing()) {
//      log.info("[HuoBi]: receive ping");
//    } else if (frame.isClose()) {
//      log.info("[HuoBi]: receive close");
//    } else if (frame.isText()) {
//      log.info("[HuoBi]: receive text msg: {}", frame.textData());
//    }
//  }

  /**
   * 判断是否为ping消息
   *
   * @param object 消息对象
   * @return 是否为ping消息
   */
  private boolean isPingMsg(JsonObject object) {
    return object.containsKey("ping");
  }

  /**
   * 回复pong消息
   */
  private void pong() {
    super.writeText("{\"pong\":" + System.currentTimeMillis() + "}");
  }

  private String toGenericSymbol(String symbol) {
    return symbol.replace("-", "")
        .replace("/", "")
        .toLowerCase();
  }
}
