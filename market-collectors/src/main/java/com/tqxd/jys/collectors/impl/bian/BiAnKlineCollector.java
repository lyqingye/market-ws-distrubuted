package com.tqxd.jys.collectors.impl.bian;


import com.tqxd.jys.collectors.impl.Collector;
import com.tqxd.jys.collectors.impl.DataReceiver;
import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.constance.BiAnPeriod;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.utils.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.ProxyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.tqxd.jys.utils.VertxUtil.jsonGetValue;

/**
 * @author ex
 */
@SuppressWarnings("Duplicates")
public class BiAnKlineCollector extends GenericWsCollector {
  private static final Logger log = LoggerFactory.getLogger(BiAnKlineCollector.class);
  /**
   * 用于存储交易对的映射
   * 币安交易对映射 -> 用户自定义交易对映射
   */
  private Map<String, String> symbolDeMapping = new HashMap<>();


  /**
   * 订阅ID
   */
  private long subIdPrefix;

  /**
   * 最新订阅id
   */
  private Long lastSubId;

  /**
   * 由于深度订阅方式不一样，新建了Verticle，用于管理Verticle的创建和销毁
   */
  private Map<String,String> depthVerticleMap = new HashMap<>();

  /**
   * 开启收集数据
   */
  @Override
  @SuppressWarnings("rawtypes")
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    boolean proxySwitch =  jsonGetValue(config(),"proxy.switch",Boolean.class);
    String proxyHost =  jsonGetValue(config(),"proxy.host",String.class);
    Integer proxyPort =  jsonGetValue(config(),"proxy.port",Integer.class);
    HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true)
              .setDefaultHost(config().getString("host")).setDefaultPort(config().getInteger("port"));
    if(proxySwitch){
      httpClientOptions.setProxyOptions(new ProxyOptions().setHost(proxyHost).setPort(proxyPort).setType(ProxyType.HTTP));
    }
    config().put(HTTP_CLIENT_OPTIONS_PARAM, httpClientOptions);
    config().put(WS_REQUEST_PATH_PARAM, config().getString("path"));
    config().put(IDLE_TIME_OUT, 5000);
    subIdPrefix = System.currentTimeMillis();
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
          log.info("[BiAn]: start success!");
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
//    if(DataType.KLINE.equals(dataType)){
//      if(null == lastSubId){
//        lastSubId = subIdPrefix;
//      }else{
//        this.unSubscribe(dataType,symbol,handler);
//      }
//    }
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future()
        .onSuccess(none -> {
          String params;
          JsonArray paramsArray = new JsonArray();
          JsonObject json = new JsonObject();
          json.put("id", subIdPrefix);
          switch (dataType) {
            case KLINE: {

              List<String> symbolList = super.listSubscribedInfo().get(DataType.KLINE);
              for (String sym : symbolList) {
                for (BiAnPeriod period : BiAnPeriod.values()) {
                  //<symbol>@kline_<interval>
                  String symbolPeriod = BiAnUtils.toKlineSub(toGenericSymbol(sym), period);
                  paramsArray.add(symbolPeriod);

                  //key:klineBNBUSDT1m value:BTC_USDT
                  String symbolPeriodDe = BiAnUtils.toKlineSymbolDeMappingKey(toGenericSymbolUpperCase(sym), period);
                  symbolDeMapping.put(symbolPeriodDe,sym);
                }
              }

              //币安k线订阅
              json.put("method", "SUBSCRIBE");
              json.put("params", paramsArray);
              super.writeText(json.toString());
              log.info("[BiAn]: subscribe: {}", json.toString());
              handler.handle(Future.succeededFuture());


              break;
            }
            case TRADE_DETAIL: {
              params = BiAnUtils.toTradeDetailSub(toGenericSymbol(symbol));
              symbolDeMapping.put(params, symbol);
              json.put("method", "SUBSCRIBE");
              json.put("params", params);
              super.writeText(json.toString());
              log.info("[BiAn]: subscribe: {}", json.toString());
              handler.handle(Future.succeededFuture());
              break;
            }
            case DEPTH: {
              subscribeDepth(symbol, handler);
              break;
            }
          }

        })
        .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  private void subscribeDepth(String symbol, Handler<AsyncResult<Void>> handler) {
    JsonObject cfg = new JsonObject()
            .put(HTTP_CLIENT_OPTIONS_PARAM,config().getValue(HTTP_CLIENT_OPTIONS_PARAM))
            .put(WS_REQUEST_PATH_PARAM, "/ws/" + toGenericSymbol(symbol) + "@depth20@1000ms")
            .put(IDLE_TIME_OUT, 5000);
    BiAnDepthCollector biAnDepthCollector = new BiAnDepthCollector(cfg);
    BiAnKlineCollector that = this;

    // 监听BiAnDepthCollector的数据到来事件
    biAnDepthCollector.addDataReceiver(new DataReceiver() {
      @Override
      public void onReceive(Collector from, DataType dataType, JsonObject obj) {
        // 将此事件传递给 BiAnKlineCollector
        // 这个事件最终会传递给 com.tqxd.jys.collectors.openapi.CollectorOpenApiImpl 做数据汇总处理
        that.unParkReceives(dataType,BiAnDataConvert.depth(obj, symbol));
      }
    });

    // 因为币安返回的数据不带有我所订阅的主题，所以我不知道这个websocket返回的数据是什么主题的数据
    // 所以一个主题只能对应一个websocket连接，这样才能区分数据是属于哪个主题，可以参考币安的接口文档
    // 以下代码是创建一个websocket连接
    vertx.deployVerticle(biAnDepthCollector,ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      }else {
        // 保存已经部署的verticle的部署ID，取消订阅的时候需要取消该verticel的部署
        depthVerticleMap.put(symbol,ar.result());
        handler.handle(Future.succeededFuture());
      }
    });
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
          String params;
          JsonArray paramsArray = new JsonArray();
          JsonObject json = new JsonObject();
          json.put("id", subIdPrefix);
          switch (dataType) {
            case KLINE: {
              for (BiAnPeriod period : BiAnPeriod.values()) {
                params = BiAnUtils.toKlineSub(toGenericSymbol(symbol), period);
                paramsArray.add(params);

                symbolDeMapping.put(params, BiAnUtils.toKlineSymbolDeMappingKey(toGenericSymbolUpperCase(symbol), period));
              }
              //币安k线订阅
              json.put("method", "UNSUBSCRIBE");
              json.put("params", paramsArray);
              super.writeText(json.toString());
              log.info("[BiAn]: subscribe: {}", json.toString());
              handler.handle(Future.succeededFuture());
              break;
            }
            case DEPTH: {
              String deployedId = depthVerticleMap.get(symbol);
              if (deployedId != null) {
                vertx.undeploy(deployedId, ar -> {
                  if (ar.succeeded()) {
                    depthVerticleMap.remove(symbol);
                    handler.handle(Future.succeededFuture());
                  }else {
                    handler.handle(Future.failedFuture(ar.cause()));
                  }
                });
              }

              break;
            }
//            case TRADE_DETAIL: {
//              unsub = HuoBiUtils.toTradeDetailSub(toGenericSymbol(symbol));
//              symbolDeMapping.put(unsub, HuoBiUtils.toTradeDetailSub(symbol));
//              json.put("unsub", unsub);
//              log.info("[BiAn]: unsubscribe: {}", unsub);
//              super.writeText(json.toString());
//              break;
//            }
          }

        })
        .onFailure(throwable -> {
          handler.handle(Future.failedFuture(throwable));
        });
  }

  @Override
  public String name() {
    return BiAnKlineCollector.class.getName();
  }

  /**
   * 描述一个收集器
   *
   * @return 收集器描述
   */
  @Override
  public String desc() {
    return "BiAn";
  }

  @Override
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    super.onFrame(client, frame);

    if(frame.isPing()){
      log.info("[BiAn] ping:{}",frame.textData());
      super.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
      return;
    }

    if (frame.isText() && frame.isFinal()) {
      JsonObject obj = new JsonObject(frame.textData());
      String e = obj.getString("e");
      // k线主题
      if ("kline".equalsIgnoreCase(e)) {
        JsonObject klineTickTemplatePayload = BiAnDataConvert.kline(obj,symbolDeMapping);
        if(null == klineTickTemplatePayload) return;
        super.unParkReceives(DataType.KLINE,klineTickTemplatePayload);
      }else if ("aggTrade".equalsIgnoreCase(e)) { //交易记录
        JsonObject trade = BiAnDataConvert.trade(obj,symbolDeMapping);
        if(null == trade) return;
        super.unParkReceives(DataType.TRADE_DETAIL,trade);
      }
      log.info("[BiAn] subscribe result:{}",frame.textData());
    }
  }


  private String toGenericSymbol(String symbol) {
    return symbol.replace("-", "")
            .replace("/", "")
            .toLowerCase();
  }

  private String toGenericSymbolUpperCase(String symbol) {
    return symbol.replace("-", "")
            .replace("/", "")
            .toUpperCase();
  }

//
//  /**
//   * 判断是否为ping消息
//   *
//   * @param object 消息对象
//   * @return 是否为ping消息
//   */
//  private boolean isPingMsg(JsonObject object) {
//    return object.containsKey("ping");
//  }
//
//  /**
//   * 回复pong消息
//   */
//  private void pong() {
//    super.writeText("{\"pong\":" + System.currentTimeMillis() + "}");
//  }


  //package com.tqxd.jys.collectors.impl;
//
//
//import com.tqxd.jys.common.payload.KlineTick;
//import com.tqxd.jys.common.payload.TemplatePayload;
//import com.tqxd.jys.constance.BiAnPeriod;
//import com.tqxd.jys.constance.DataType;
//import com.tqxd.jys.constance.DepthLevel;
//import com.tqxd.jys.constance.Period;
//import com.tqxd.jys.utils.BiAnUtils;
//import com.tqxd.jys.utils.ChannelUtil;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.core.Vertx;
//import io.vertx.core.buffer.Buffer;
//import io.vertx.core.http.HttpClient;
//import io.vertx.core.http.HttpClientOptions;
//import io.vertx.core.http.WebSocket;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import io.vertx.core.net.ProxyOptions;
//import io.vertx.core.net.ProxyType;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.math.BigDecimal;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//import java.util.concurrent.TimeUnit;
//import java.util.function.BiConsumer;
//
///**
// * @author ex
// */
//@SuppressWarnings("Duplicates")
//public class BiAnKlineCollector2 extends GenericWsCollector {
//  private static final Logger log = LoggerFactory.getLogger(BiAnKlineCollector2.class);
//
//  /**
//   * vertx 实例
//   */
//  private Vertx   vertx;
//
//  /**
//   * 用于存储交易对的映射
//   * 火币交易对映射 -> 用户自定义交易对映射
//   */
//  private Map<String, String> symbolDeMapping = new HashMap<>();
//
//  /**
//   * 数据消费者
//   */
//  private BiConsumer<DataType, JsonObject> consumer;
//
//  /**
//   * 额外参数
//   */
//  private Config config;
//
//  /**
//   * websocket实例
//   */
//  private WebSocket ws;
//
//  /**
//   * http 客户端
//   */
//  private HttpClient hc;
//
//  /**
//   * 订阅ID
//   */
//  private String subIdPrefix;
//
//  /**
//   * 部署一个收集器
//   *
//   * @param vertx    vertx 实例
//   * @param consumer 数据消费器
//   * @param args     附加参数 (可以为空)
//   * @return 是否部署成功
//   * @throws Exception 如果部署失败
//   */
//  @Override
//  public boolean deploy(Vertx vertx,
//                        BiConsumer<DataType, JsonObject> consumer,
//                        JsonObject args) {
//    boolean result = super.deploy(vertx, consumer, args);
//
//    if (!result) {
//      return false;
//    }
//
//    if (args != null) {
//      this.config = args.mapTo(Config.class);
//    }
//
//    if (this.config == null ||
//        this.config.getHost() == null ||
//        this.config.reqUrl == null) {
//      this.config = new Config();
//    }
//
//    this.vertx = vertx;
//    this.consumer = consumer;
//    return true;
//  }
//
//  /**
//   * 开启收集数据
//   *
//   * @param handler 回调
//   */
//  @Override
//  public void start(Handler<AsyncResult<Boolean>> handler) {
//    subIdPrefix = String.valueOf(System.currentTimeMillis());
//    super.start(ar -> {
//      if (ar.succeeded()) {
//        HttpClientOptions options = new HttpClientOptions().setSsl(true).setDefaultHost("stream.binance.com").setDefaultPort(9443)
//                .setProxyOptions(new ProxyOptions().setHost("localhost").setPort(4780).setType(ProxyType.HTTP));
//        this.hc = this.vertx.createHttpClient(options);
//        BiAnKlineCollector2 that = this;
//        // 创建websocket 链接
//        hc.webSocket(this.config.reqUrl, wsAr -> {
//          if (wsAr.succeeded()) {
//            refreshLastReceiveTime();
//            that.ws = wsAr.result();
//            this.registerMsgHandler(that.ws);
//            // 重新订阅
//            super.listSubscribedInfo().forEach(((collectDataType, symbols) -> {
//              if (symbols != null) {
//                for (String symbol : symbols) {
//                  this.subscribe(collectDataType, symbol);
//                }
//              }
//            }));
//            // 启用空闲链路检测
//            startIdleChecker();
//            handler.handle(Future.succeededFuture(true));
//          } else {
//            handler.handle(Future.failedFuture(wsAr.cause()));
//          }
//        });
//      } else {
//        handler.handle(ar);
//      }
//    });
//
//  }
//
//
//  /**
//   * 取消部署收集器
//   *
//   * @param args 附加参数可以为空
//   * @return 如果取消部署失败
//   * @throws Exception 如果取消部署失败
//   */
//  @Override
//  public boolean unDeploy(JsonObject args) {
//    boolean result = super.unDeploy(args);
//    if (this.hc != null) {
//      try {
//        this.hc.close();
//        this.hc = null;
//        return result;
//      } catch (Exception e) {
//        result = false;
//      }
//    }
//    return result;
//  }
//
//  @Override
//  public boolean queryHistory(String symbol, Period period, long from, long to) {
//    return false;
//  }
//
//  /**
//   * 停止数据收集
//   *
//   * @return 是否停止成功
//   */
//  @Override
//  public void stop(Handler<AsyncResult<Void>> handler) {
//    super.stop(ar -> {
//      if (ar.succeeded()) {
//        if (this.hc != null) {
//          try {
//            this.hc.close().onComplete(handler);
//            this.hc = null;
//            // 停止空闲链路检测
//            stopIdleChecker();
//          } catch (Exception e) {
//            handler.handle(Future.failedFuture(e));
//          }
//        }
//      } else {
//        handler.handle(Future.failedFuture(ar.cause()));
//      }
//    });
//  }
//
//  /**
//   * 获取websocket实例
//   *
//   * @return 实例
//   */
//  @Override
//  public WebSocket ws() {
//    return this.ws;
//  }
//
//  /**
//   * 订阅一个交易对
//   *
//   * @param dataType 数据收集类型
//   * @param symbol   交易对
//   * @return 是否订阅成功
//   */
//  @Override
//  public boolean subscribe(DataType dataType, String symbol) {
//    boolean result = super.subscribe(dataType, symbol);
//    if (!result) {
//      return false;
//    }
//
//    if (this.ws != null && !this.ws.isClosed()) {
//      String params;
//      JsonArray paramsArray = new JsonArray();
//      JsonObject json = new JsonObject();
//      json.put("id", System.currentTimeMillis());
//      switch (dataType) {
//        case KLINE: {
//          //订阅不同档位k线
//          for (BiAnPeriod period : BiAnPeriod.values()) {
//            params = BiAnUtils.toKlineBiAnSub(toGenericSymbol(symbol), period);
//            paramsArray.add(params);
//            symbolDeMapping.put(params, );
//          }
//          //币安k线订阅
//          json.put("method", "SUBSCRIBE");
//          json.put("params", paramsArray);
//          this.ws.writeTextMessage(json.toString());
//          //params = {"id":1620696851384,"method":"SUBSCRIBE","params":["btcusdt@kline_1m","btcusdt@kline_5m","btcusdt@kline_15m","btcusdt@kline_30m","btcusdt@kline_60m","btcusdt@kline_4h","btcusdt@kline_1d","btcusdt@kline_1w"]}
//          log.info("[BiAn]: subscribe: {}", json.toString());
//          break;
//        }
////        case DEPTH: {
////          // 只订阅深度为0的
////          sub = HuoBiUtils.toDepthSub(toGenericSymbol(symbol), DepthLevel.step0);
////          symbolDeMapping.put(sub, HuoBiUtils.toDepthSub(symbol, DepthLevel.step0));
////          json.put("sub", sub);
////          this.ws.writeTextMessage(json.toString());
////          log.info("[HuoBi]: subscribe: {}", sub);
////          break;
////        }
////        case TRADE_DETAIL: {
////          sub = HuoBiUtils.toTradeDetailSub(toGenericSymbol(symbol));
////          symbolDeMapping.put(sub, HuoBiUtils.toTradeDetailSub(symbol));
////          json.put("sub", sub);
////          this.ws.writeTextMessage(json.toString());
////          log.info("[HuoBi]: subscribe: {}", sub);
////          break;
////        }
//        default: {
//          result = false;
//        }
//      }
//
//    } else {
//      result = false;
//    }
//    return result;
//  }
//
//  /**
//   * 取消订阅一个交易对
//   *
//   * @param dataType 数据收集类型
//   * @param symbol   交易对
//   * @return 是否取消订阅成功
//   */
//  @Override
//  public boolean unSubscribe(DataType dataType, String symbol) {
//    boolean result = super.unSubscribe(dataType, symbol);
//    if (!result) {
//      return false;
//    }
//    if (this.ws != null && !this.ws.isClosed()) {
//      String id = subIdPrefix + symbol;
//      String unsub = null;
//      JsonObject json = new JsonObject();
//      json.put("id", id);
//      switch (dataType) {
//        case KLINE: {
//          // 只订阅 1min的交易
//          for (BiAnPeriod period : BiAnPeriod.values()) {
//            unsub = BiAnUtils.toKlineSub(toGenericSymbol(symbol), period);
//            symbolDeMapping.put(unsub, BiAnUtils.toKlineSub(symbol, period));
//            json.put("unsub", unsub);
//            log.info("[BiAn]: unsubscribe: {}", unsub);
//            this.ws.writeTextMessage(json.toString());
//          }
//          break;
//        }
//        case DEPTH: {
//          // 只订阅深度为0的
//          unsub = BiAnUtils.toDepthSub(toGenericSymbol(symbol), DepthLevel.step0);
//          symbolDeMapping.put(unsub, BiAnUtils.toDepthSub(symbol, DepthLevel.step0));
//          json.put("unsub", unsub);
//          log.info("[BiAn]: unsubscribe: {}", unsub);
//          this.ws.writeTextMessage(json.toString());
//          break;
//        }
//        case TRADE_DETAIL: {
//          unsub = BiAnUtils.toTradeDetailSub(toGenericSymbol(symbol));
//          symbolDeMapping.put(unsub, BiAnUtils.toTradeDetailSub(symbol));
//          json.put("unsub", unsub);
//          log.info("[BiAn]: unsubscribe: {}", unsub);
//          this.ws.writeTextMessage(json.toString());
//          break;
//        }
//        default: {
//          result = false;
//        }
//      }
//    } else {
//      result = false;
//    }
//    return result;
//  }
//
//  @Override
//  public String name() {
//    return BiAnKlineCollector2.class.getName();
//  }
//
//  /**
//   * 描述一个收集器
//   *
//   * @return 收集器描述
//   */
//  @Override
//  public String desc() {
//    return "火币数据收集器";
//  }
//
//  /**
//   * 注册websocket消息处理事件
//   *
//   * @param ws
//   */
//  private void registerMsgHandler(WebSocket ws) {
//    ws.frameHandler(frame -> {
//      refreshLastReceiveTime();
//
//      //服务端发送ping帧时需要返回pong帧
//      if(frame.isPing()){
//        log.info("[BiAn] ping:{}",frame.textData());
//
//        ws.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())),ar -> {
//            if(ar.failed()){
//              ar.cause().printStackTrace();
//            }
//        });
//        return;
//      }
//      if (frame.isText() && frame.isFinal()) {
//        JsonObject obj = new JsonObject(frame.textData());
//        if (isPingMsg(obj)) {
//          this.writePong(ws);
//        } else {
//          String e = obj.getString("e");
//          obj.put("ch", symbolDeMapping.get(e));
//          // k线主题
//          if ("kline".equalsIgnoreCase(e)) {
//            TemplatePayload<KlineTick> of = getKlineTickTemplatePayload(obj);
//            if (of == null) return;
//            consumer.accept(DataType.KLINE,JsonObject.mapFrom(of));
//          }else if (ChannelUtil.isDepthChannel(e)) {
//            consumer.accept(DataType.DEPTH,obj);
//          }else if (ChannelUtil.isTradeDetailChannel(e)) {
//            consumer.accept(DataType.TRADE_DETAIL,obj);
//          }
//        }
//        log.info(frame.textData());
//      }
//
//      if (frame.isBinary() && frame.isFinal()) {
//        log.info(frame.binaryData().toJson().toString());
//      }
//
//
//      // 处理二进制帧并且确保是最终帧
////      if (frame.isBinary() && frame.isFinal()) {
////        JsonObject obj = (JsonObject) frame.binaryData().toJson();
////        if (isPingMsg(obj)) {
////          this.writePong(ws);
////        } else {
////          String ch = obj.getString("ch");
////          // 取消交易对映射
////          obj.put("ch", symbolDeMapping.get(ch));
////          // k线主题
////          if (ChannelUtil.isKLineChannel(ch)) {
////            consumer.accept(DataType.KLINE, obj);
////          }else if (ChannelUtil.isDepthChannel(ch)) {
////            consumer.accept(DataType.DEPTH,obj);
////          }else if (ChannelUtil.isTradeDetailChannel(ch)) {
////            consumer.accept(DataType.TRADE_DETAIL,obj);
////          }
////        }
////      }
//    });
//  }
//
//  private TemplatePayload<KlineTick> getKlineTickTemplatePayload(JsonObject obj) {
//    JsonObject k = obj.getJsonObject("k");
//    KlineTick tick = new KlineTick();
//    tick.setId(k.getLong("t"));
//    tick.setAmount(new BigDecimal(k.getString("V")));
//    tick.setCount(Integer.valueOf(k.getString("n")));
//    tick.setOpen(new BigDecimal(k.getString("o")));
//    tick.setClose(new BigDecimal(k.getString("c")));
//    tick.setLow(new BigDecimal(k.getString("l")));
//    tick.setHigh(new BigDecimal(k.getString("h")));
//    tick.setVol(new BigDecimal(k.getString("Q")));
//    Period period = BiAnPeriod.containsSymbol(k.getString("i"));
//    if(null == period){
//      log.info("[BiAn] BiAnPeriod 无法适配 i:{}",k.getString("i"));
//      return null;
//    }
//    return TemplatePayload.of(ChannelUtil.buildKLineTickChannel("BTC-USDT", period), tick);
//  }
//
//  /**
//   * 判断是否为ping消息
//   *
//   * @param object 消息对象
//   * @return 是否为ping消息
//   */
//  private boolean isPingMsg(JsonObject object) {
//    return object.containsKey("ping");
//  }
//
//  /**
//   * 判断消息是否为tick消息
//   *
//   * @param object 消息对象
//   * @return 是否为tick消息
//   */
//  private boolean isTickMsg(JsonObject object) {
//    return object.containsKey("tick");
//  }
//
//  /**
//   * 回复pong消息
//   *
//   * @param ws websocket
//   */
//  private void writePong(WebSocket ws) {
//    ws.writeTextMessage("{\"pong\":" + System.currentTimeMillis() + "}");
//  }
//
//  private String toGenericSymbol(String symbol) {
//    return symbol.replace("-", "")
//        .replace("/", "")
//        .toLowerCase();
//  }
//
//  private static class Config {
//    /**
//     * 域名
//     */
//    private String host = "https://stream.binance.com";
//
//    /**
//     * 请求url
//     */
//    private String reqUrl = "/ws";
//
//    /**
//     * 心跳周期 默认5秒
//     */
//    private Long heartbeat = TimeUnit.SECONDS.toMillis(5);
//
//    public String getHost() {
//      return host;
//    }
//
//    public void setHost(String host) {
//      this.host = host;
//    }
//
//    public String getReqUrl() {
//      return reqUrl;
//    }
//
//    public void setReqUrl(String reqUrl) {
//      this.reqUrl = reqUrl;
//    }
//
//    public Long getHeartbeat() {
//      return heartbeat;
//    }
//
//    public void setHeartbeat(Long heartbeat) {
//      this.heartbeat = heartbeat;
//    }
//  }
//}


}
