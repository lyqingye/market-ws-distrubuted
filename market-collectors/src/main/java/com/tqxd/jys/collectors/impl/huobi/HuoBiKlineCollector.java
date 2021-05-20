package com.tqxd.jys.collectors.impl.huobi;

import com.tqxd.jys.collectors.impl.GenericWsCollector;
import com.tqxd.jys.collectors.impl.huobi.helper.HuoBiUtils;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.utils.GZIPUtils;
import io.vertx.core.*;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tqxd.jys.collectors.impl.huobi.helper.HuoBiUtils.*;

/**
 * @author ex
 */
@SuppressWarnings("Duplicates")
public class HuoBiKlineCollector extends GenericWsCollector {
  private static final Logger log = LoggerFactory.getLogger(HuoBiKlineCollector.class);
  /**
   * 用于存储交易对的映射 火币交易对映射 -> 用户自定义交易对映射
   */
  private Map<String, String> channelDeMapping = new HashMap<>();

  /**
   * 开启收集数据
   */
  @Override
  @SuppressWarnings("rawtypes")
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    HttpClientOptions httpClientOptions = new HttpClientOptions().setDefaultHost(config().getString("host"));
    config().put(HTTP_CLIENT_OPTIONS_PARAM, httpClientOptions);
    config().put(WS_REQUEST_PATH_PARAM, config().getString("path"));
    config().put(IDLE_TIME_OUT, config().getLong("idle-time-out", 5000L));
    Promise<Void> promise = Promise.promise();
    super.start(promise);
    promise.future().compose(none -> {
      List<Future> futures = new ArrayList<>();
      super.listSubscribedInfo().forEach(((collectDataType, symbols) -> {
        if (symbols != null) {
          for (String symbol : symbols) {
            futures.add(this.subscribe(collectDataType, symbol));
          }
        }
      }));
      return CompositeFuture.any(futures);
    }).onSuccess(ar -> {
      log.info("[HuoBi]: start success!");
      startPromise.complete();
    }).onFailure(startPromise::fail);
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
    promise.future().onSuccess(none -> {
      switch (dataType) {
        case KLINE: {
          for (Period period : Period.values()) {
            String sourceCh = HuoBiUtils.buildKLIneChannel(symbol, period);
            String huoBiCh = HuoBiUtils.buildKLIneChannel(toHuoBiSymbol(symbol), period);
            putChannelDeMapping(sourceCh, huoBiCh);
            super.writeText(HuoBiUtils.buildKLineSubReq(System.currentTimeMillis(), huoBiCh));
            log.info("[HuoBi]: subscribe: {}", huoBiCh);
          }
          break;
        }
        case DEPTH: {
          String sourceCh = buildDepthChannel(symbol, DepthLevel.step0);
          String huoBiCh = buildDepthChannel(toHuoBiSymbol(symbol), DepthLevel.step0);
          putChannelDeMapping(sourceCh, huoBiCh);
          super.writeText(buildDepthSubReq(System.currentTimeMillis(), huoBiCh, 20, 20));
          log.info("[HuoBi]: subscribe: {}", huoBiCh);
          break;
        }
        case TRADE_DETAIL: {
          String sourceCh = buildTradeDetailChannel(symbol);
          String huoBiCh = buildTradeDetailChannel(toHuoBiSymbol(symbol));
          putChannelDeMapping(sourceCh, huoBiCh);
          super.writeText(buildTradeDetailSubReq(System.currentTimeMillis(), huoBiCh));
          log.info("[HuoBi]: subscribe: {}", huoBiCh);
          break;
        }
        default: {
          handler.handle(Future.failedFuture("unknown data type for: " + dataType));
          break;
        }
      }
      handler.handle(Future.succeededFuture());
    }).onFailure(throwable -> {
      handler.handle(Future.failedFuture(throwable));
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
    promise.future().onSuccess(none -> {
      String huoBiSymbol = toHuoBiSymbol(symbol);
      switch (dataType) {
        case KLINE: {
          for (Period period : Period.values()) {
            String req = buildKLineUnsubReq(System.currentTimeMillis(), huoBiSymbol, period);
            super.writeText(req);
            log.info("[HuoBi]: unsubscribe: {}", req);
          }
          break;
        }
        case DEPTH: {
          String req = buildDepthUnsubReq(System.currentTimeMillis(), huoBiSymbol, DepthLevel.step0, 20, 20);
          super.writeText(req);
          log.info("[HuoBi]: unsubscribe: {}", req);
          break;
        }
        case TRADE_DETAIL: {
          String req = buildTradeDetailUnsubReq(System.currentTimeMillis(), huoBiSymbol);
          super.writeText(req);
          log.info("[HuoBi]: unsubscribe: {}", req);
          break;
        }
        default: {
          handler.handle(Future.failedFuture("unknown data type for: " + dataType));
          break;
        }
      }
      handler.handle(Future.succeededFuture());
    }).onFailure(throwable -> {
      handler.handle(Future.failedFuture(throwable));
    });
  }

  @Override
  public String name() {
    return HuoBiKlineCollector.class.getName();
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

  @Override
  public void onFrame(WebSocket client, WebSocketFrame frame) {
    super.onFrame(client, frame);
    if (frame.isBinary() && frame.isFinal()) {
      try {
        // GZIP解压
        byte[] data = GZIPUtils.fastDecompress(frame.binaryData());
        JsonObject obj = (JsonObject) Json.decodeValue(new String(data, StandardCharsets.UTF_8));
        if (isPingMsg(obj)) {
          this.pong();
        } else {
          String huoBiChannel = obj.getString("ch");
          obj.put("ch", deChannelMapping(huoBiChannel));
          if (ChannelUtil.isKLineChannel(huoBiChannel)) {
            unParkReceives(DataType.KLINE, obj);
          } else if (ChannelUtil.isDepthChannel(huoBiChannel)) {
            unParkReceives(DataType.DEPTH, obj);
          } else if (ChannelUtil.isTradeDetailChannel(huoBiChannel)) {
            unParkReceives(DataType.TRADE_DETAIL, obj);
          }
        }
      } catch (IOException e) {
        log.warn("[HuoBi]: decompress fail! cause by: {}:", e.getMessage());
        e.printStackTrace();
      }
    }
  }

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

  private void putChannelDeMapping(String source, String huoBi) {
    channelDeMapping.put(huoBi, source);
  }

  private String deChannelMapping(String huoBi) {
    return channelDeMapping.get(huoBi);
  }

  private String toHuoBiSymbol(String symbol) {
    return symbol.replace("-", "").replace("/", "").toLowerCase();
  }
}
