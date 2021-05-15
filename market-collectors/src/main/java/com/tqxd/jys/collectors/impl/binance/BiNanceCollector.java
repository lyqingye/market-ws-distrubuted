package com.tqxd.jys.collectors.impl.binance;


import com.tqxd.jys.collectors.impl.Collector;
import com.tqxd.jys.collectors.impl.DataReceiver;
import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.binance.helper.BiNanceDataConvert;
import com.tqxd.jys.collectors.impl.binance.helper.BiNanceRequestUtils;
import com.tqxd.jys.collectors.impl.binance.sub.BiNanceDepthCollector;
import com.tqxd.jys.constance.DataType;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.ProxyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.tqxd.jys.utils.VertxUtil.jsonGetValue;

/**
 * 币安行情数据收集器
 * <p>
 * 1. k线数据采用发送订阅请求方式，参考文档 https://binance-docs.github.io/apidocs/spot/cn/#3c863d56da
 * 2. 深度数据采用websocket path 连接方式, 参考文档 https://binance-docs.github.io/apidocs/spot/cn/#6ae7c2b506
 * 3. 成交记录采用订阅请求方式, 参考文档 https://binance-docs.github.io/apidocs/spot/cn/#2b149598d9
 * <p>
 * 币安收集器配置为:
 * <p>
 * <pre>
 * binance:
 *   clazz: com.tqxd.jys.collectors.impl.binance.BiNanceCollector
 *   config:
 *     proxy:
 *       switch: true
 *       host: 127.0.0.1
 *       port: 4780
 *     host: stream.binance.com
 *     port: 9443
 *     path: /ws
 * </pre>
 * <p>
 * proxy.switch 为代理的开关，如果是true则说明需要走代理，false 为不走代理
 * <p>
 * 注: 其中proxy为代理，币安需要翻墙，代理默认采用 http 代理
 *
 * @author lyqingye
 */
@SuppressWarnings("Duplicates")
public class BiNanceCollector extends GenericWsCollector {
  private static final Logger log = LoggerFactory.getLogger(BiNanceCollector.class);
  /**
   * 用于存储交易对的映射
   * 币安交易对映射 -> 用户自定义交易对映射
   */
  private Map<String, String> symbolDeMapping = new HashMap<>();

  /**
   * 上一次订阅id
   */
  private Long subscribeId;

  /**
   * 由于深度订阅方式不一样，新建了Verticle，用于管理Verticle的创建和销毁
   */
  private Map<String, String> depthCollectorVerticleMap = new HashMap<>();

  /**
   * 开启收集数据
   */
  @Override
  @SuppressWarnings("rawtypes")
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    boolean proxySwitch = jsonGetValue(config(), "proxy.switch", Boolean.class);
    String proxyHost = jsonGetValue(config(), "proxy.host", String.class);
    Integer proxyPort = jsonGetValue(config(), "proxy.port", Integer.class);
    HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true)
      .setDefaultHost(config().getString("host"))
      .setDefaultPort(config().getInteger("port"));
    if (proxySwitch) {
      httpClientOptions.setProxyOptions(new ProxyOptions().setHost(proxyHost).setPort(proxyPort).setType(ProxyType.HTTP));
    }
    config().put(HTTP_CLIENT_OPTIONS_PARAM, httpClientOptions);
    config().put(WS_REQUEST_PATH_PARAM, config().getString("path"));
    config().put(IDLE_TIME_OUT, TimeUnit.SECONDS.toMillis(30));
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
        return CompositeFuture.any(futures);
      })
      .onSuccess(ar -> {
        log.info("[BiNance]: start success!");
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
  public synchronized void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future()
      .onSuccess(none -> {
        subscribeId = System.currentTimeMillis();
        String biNanceSymbol = toBiNanceSymbol(symbol);
        putSymbolMapping(symbol, biNanceSymbol);
        log.info("[BiNance]: subscribe: {} {}", dataType, symbol);
        switch (dataType) {
          case KLINE: {
            super.writeText(BiNanceRequestUtils.buildSubscribeKLineReq(subscribeId, biNanceSymbol));
            handler.handle(Future.succeededFuture());
            break;
          }
          case TRADE_DETAIL: {
            super.writeText(BiNanceRequestUtils.buildSubscribeTradeDetailReq(subscribeId, biNanceSymbol));
            handler.handle(Future.succeededFuture());
            break;
          }
          case DEPTH: {
            this.subscribeDepth(symbol, handler);
            break;
          }
        }
      })
      .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  private void subscribeDepth(String symbol, Handler<AsyncResult<Void>> handler) {
    synchronized (this) {
      if (depthCollectorVerticleMap.containsKey(symbol)) {
        return;
      }
    }
    JsonObject cfg = new JsonObject()
      .put(HTTP_CLIENT_OPTIONS_PARAM, config().getValue(HTTP_CLIENT_OPTIONS_PARAM))
      .put(WS_REQUEST_PATH_PARAM, "/ws/" + toBiNanceSymbol(symbol) + "@depth20@1000ms")
      .put(IDLE_TIME_OUT, 5000);
    BiNanceDepthCollector biNanceDepthCollector = new BiNanceDepthCollector(cfg);
    BiNanceCollector that = this;
    biNanceDepthCollector.addDataReceiver(new DataReceiver() {
      @Override
      public void onReceive(Collector from, DataType dataType, JsonObject obj) {
        that.unParkReceives(dataType, BiNanceDataConvert.convertToDepth(obj, symbol));
      }
    });
    vertx.deployVerticle(biNanceDepthCollector, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        depthCollectorVerticleMap.put(symbol, ar.result());
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
  public synchronized void unSubscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.unSubscribe(dataType, symbol, promise);
    promise.future()
      .onSuccess(none -> {
        String biNanceSymbol = toBiNanceSymbol(symbol);
        log.info("[BiNance]: unsubscribe: {} {}", dataType, symbol);
        switch (dataType) {
          case KLINE: {
            super.writeText(BiNanceRequestUtils.buildUnSubscribeKLineReq(subscribeId, biNanceSymbol));
            handler.handle(Future.succeededFuture());
            break;
          }
          case TRADE_DETAIL: {
            super.writeText(BiNanceRequestUtils.buildUnSubscribeTradeDetailReq(subscribeId, biNanceSymbol));
            handler.handle(Future.succeededFuture());
            break;
          }
          case DEPTH: {
            String deployedId = depthCollectorVerticleMap.get(symbol);
            if (deployedId != null) {
              vertx.undeploy(deployedId, ar -> {
                if (ar.succeeded()) {
                  depthCollectorVerticleMap.remove(symbol);
                  handler.handle(Future.succeededFuture());
                } else {
                  handler.handle(Future.failedFuture(ar.cause()));
                }
              });
            }
            break;
          }
        }
      })
      .onFailure(throwable -> {
        handler.handle(Future.failedFuture(throwable));
      });
  }

  @Override
  public String name() {
    return BiNanceCollector.class.getName();
  }

  /**
   * 描述一个收集器
   *
   * @return 收集器描述
   */
  @Override
  public String desc() {
    return "币安数据收集器";
  }

  @Override
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    super.onFrame(client, frame);

    if (frame.isPing()) {
      log.info("[BiNance] ping:{}", frame.textData());
      super.writePong(Buffer.buffer(String.valueOf(System.currentTimeMillis())));
      return;
    }

    if (frame.isText() && frame.isFinal()) {
      BiNanceDataConvert.smartConvertTo((JsonObject) Json.decodeValue(frame.textData()), this::deSymbolMapping, super::unParkReceives);
    }
  }

  private void putSymbolMapping(String source, String biNance) {
    symbolDeMapping.put(biNance, source);
  }

  private String deSymbolMapping(String biNance) {
    return symbolDeMapping.get(biNance);
  }

  private String toBiNanceSymbol(String symbol) {
    return symbol.replace("-", "")
      .replace("/", "")
      .toLowerCase();
  }
}
