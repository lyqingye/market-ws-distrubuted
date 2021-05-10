package com.tqxd.jys.collectors.impl;


import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.DepthLevel;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.utils.GZIPUtils;
import com.tqxd.jys.utils.HuoBiUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author ex
 */
@SuppressWarnings("Duplicates")
public class HuoBiKlineCollector extends GenericWsCollector {
  private static final Logger log = LoggerFactory.getLogger(HuoBiKlineCollector.class);

  /**
   * vertx 实例
   */
  private Vertx vertx;

  /**
   * 用于存储交易对的映射
   * 火币交易对映射 -> 用户自定义交易对映射
   */
  private Map<String, String> symbolDeMapping = new HashMap<>();

  /**
   * 数据消费者
   */
  private BiConsumer<DataType, JsonObject> consumer;

  /**
   * 额外参数
   */
  private Config config;

  /**
   * websocket实例
   */
  private WebSocket ws;

  /**
   * http 客户端
   */
  private HttpClient hc;

  /**
   * 订阅ID
   */
  private String subIdPrefix;

  /**
   * 部署一个收集器
   *
   * @param vertx    vertx 实例
   * @param consumer 数据消费器
   * @param args     附加参数 (可以为空)
   * @return 是否部署成功
   * @throws Exception 如果部署失败
   */
  @Override
  public boolean deploy(Vertx vertx,
                        BiConsumer<DataType, JsonObject> consumer,
                        JsonObject args) {
    boolean result = super.deploy(vertx, consumer, args);

    if (!result) {
      return false;
    }

    if (args != null) {
      this.config = args.mapTo(Config.class);
    }

    if (this.config == null ||
        this.config.getHost() == null ||
        this.config.reqUrl == null) {
      this.config = new Config();
    }

    this.vertx = vertx;
    this.consumer = consumer;
    return true;
  }

  /**
   * 开启收集数据
   *
   * @param handler 回调
   */
  @Override
  public void start(Handler<AsyncResult<Boolean>> handler) {
    subIdPrefix = UUID.randomUUID().toString();
    super.start(ar -> {
      if (ar.succeeded()) {
        this.hc = this.vertx.createHttpClient();
        HuoBiKlineCollector that = this;
        // 创建websocket 链接
        hc.webSocket(this.config.host, this.config.reqUrl, wsAr -> {
          if (wsAr.succeeded()) {
            refreshLastReceiveTime();
            that.ws = wsAr.result();
            this.registerMsgHandler(that.ws);
            // 重新订阅
            super.listSubscribedInfo().forEach(((collectDataType, symbols) -> {
              if (symbols != null) {
                for (String symbol : symbols) {
                  this.subscribe(collectDataType, symbol);
                }
              }
            }));
            // 启用空闲链路检测
            startIdleChecker();
            handler.handle(Future.succeededFuture(true));
          } else {
            handler.handle(Future.failedFuture(wsAr.cause()));
          }
        });
      } else {
        handler.handle(ar);
      }
    });

  }


  /**
   * 取消部署收集器
   *
   * @param args 附加参数可以为空
   * @return 如果取消部署失败
   * @throws Exception 如果取消部署失败
   */
  @Override
  public boolean unDeploy(JsonObject args) {
    boolean result = super.unDeploy(args);
    if (this.hc != null) {
      try {
        this.hc.close();
        this.hc = null;
        return result;
      } catch (Exception e) {
        result = false;
      }
    }
    return result;
  }

  /**
   * 停止数据收集
   *
   * @return 是否停止成功
   */
  @Override
  public void stop(Handler<AsyncResult<Void>> handler) {
    super.stop(ar -> {
      if (ar.succeeded()) {
        if (this.hc != null) {
          try {
            this.hc.close().onComplete(handler);
            this.hc = null;
            // 停止空闲链路检测
            stopIdleChecker();
            handler.handle(Future.succeededFuture());
          } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
          }
        }
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * 获取websocket实例
   *
   * @return 实例
   */
  @Override
  public WebSocket ws() {
    return this.ws;
  }

  /**
   * 订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否订阅成功
   */
  @Override
  public boolean subscribe(DataType dataType, String symbol) {
    boolean result = super.subscribe(dataType, symbol);
    if (!result) {
      return false;
    }

    if (this.ws != null && !this.ws.isClosed()) {
      String id = subIdPrefix + symbol;
      String sub = null;
      JsonObject json = new JsonObject();
      json.put("id", id);
      switch (dataType) {
        case KLINE: {
          // 只订阅 1min的交易
          for (Period period : Period.values()) {
            sub = HuoBiUtils.toKlineSub(toGenericSymbol(symbol), period);
            symbolDeMapping.put(sub, HuoBiUtils.toKlineSub(symbol, period));
            json.put("sub", sub);
            this.ws.writeTextMessage(json.toString());
            log.info("[HuoBi]: subscribe: {}", sub);
          }
          break;
        }
        case DEPTH: {
          // 只订阅深度为0的
          sub = HuoBiUtils.toDepthSub(toGenericSymbol(symbol), DepthLevel.step0);
          symbolDeMapping.put(sub, HuoBiUtils.toDepthSub(symbol, DepthLevel.step0));
          json.put("sub", sub);
          this.ws.writeTextMessage(json.toString());
          log.info("[HuoBi]: subscribe: {}", sub);
          break;
        }
        case TRADE_DETAIL: {
          sub = HuoBiUtils.toTradeDetailSub(toGenericSymbol(symbol));
          symbolDeMapping.put(sub, HuoBiUtils.toTradeDetailSub(symbol));
          json.put("sub", sub);
          this.ws.writeTextMessage(json.toString());
          log.info("[HuoBi]: subscribe: {}", sub);
          break;
        }
        default: {
          result = false;
        }
      }

    } else {
      result = false;
    }
    return result;
  }

  /**
   * 取消订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否取消订阅成功
   */
  @Override
  public boolean unSubscribe(DataType dataType, String symbol) {
    boolean result = super.unSubscribe(dataType, symbol);
    if (!result) {
      return false;
    }
    if (this.ws != null && !this.ws.isClosed()) {
      String id = subIdPrefix + symbol;
      String unsub = null;
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
            this.ws.writeTextMessage(json.toString());
          }
          break;
        }
        case DEPTH: {
          // 只订阅深度为0的
          unsub = HuoBiUtils.toDepthSub(toGenericSymbol(symbol), DepthLevel.step0);
          symbolDeMapping.put(unsub, HuoBiUtils.toDepthSub(symbol, DepthLevel.step0));
          json.put("unsub", unsub);
          log.info("[HuoBi]: unsubscribe: {}", unsub);
          this.ws.writeTextMessage(json.toString());
          break;
        }
        case TRADE_DETAIL: {
          unsub = HuoBiUtils.toTradeDetailSub(toGenericSymbol(symbol));
          symbolDeMapping.put(unsub, HuoBiUtils.toTradeDetailSub(symbol));
          json.put("unsub", unsub);
          log.info("[HuoBi]: unsubscribe: {}", unsub);
          this.ws.writeTextMessage(json.toString());
          break;
        }
        default: {
          result = false;
        }
      }
    } else {
      result = false;
    }
    return result;
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

  /**
   * 注册websocket消息处理事件
   *
   * @param ws
   */
  private void registerMsgHandler(WebSocket ws) {
    ws.frameHandler(frame -> {
      refreshLastReceiveTime();
      // 处理二进制帧并且确保是最终帧
      if (frame.isBinary() && frame.isFinal()) {
        GZIPUtils.decompressAsync(vertx, frame.binaryData().getBytes())
            .onSuccess(data -> {
              JsonObject obj = (JsonObject) Json.decodeValue(new String(data, StandardCharsets.UTF_8));
              // 如果是 ping 消息则需要回复 pong
              if (isPingMsg(obj)) {
                this.writePong(ws);
              } else {
                String ch = obj.getString("ch");
                // 取消交易对映射
                obj.put("ch", symbolDeMapping.get(ch));
                // k线主题
                if (ChannelUtil.isKLineChannel(ch)) {
                  consumer.accept(DataType.KLINE, obj);
                }else if (ChannelUtil.isDepthChannel(ch)) {
                  consumer.accept(DataType.DEPTH,obj);
                }else if (ChannelUtil.isTradeDetailChannel(ch)) {
                  consumer.accept(DataType.TRADE_DETAIL,obj);
                }
              }
            })
            .onFailure(Throwable::printStackTrace);
      }
    });
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
   * 判断消息是否为tick消息
   *
   * @param object 消息对象
   * @return 是否为tick消息
   */
  private boolean isTickMsg(JsonObject object) {
    return object.containsKey("tick");
  }

  /**
   * 回复pong消息
   *
   * @param ws websocket
   */
  private void writePong(WebSocket ws) {
    ws.writeTextMessage("{\"pong\":" + System.currentTimeMillis() + "}");
  }

  private String toGenericSymbol(String symbol) {
    return symbol.replace("-", "")
        .replace("/", "")
        .toLowerCase();
  }

  private static class Config {
    /**
     * 域名
     */
    private String host = "api.huobiasia.vip";

    /**
     * 请求url
     */
    private String reqUrl = "/ws";

    /**
     * 心跳周期 默认5秒
     */
    private Long heartbeat = TimeUnit.SECONDS.toMillis(5);

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public String getReqUrl() {
      return reqUrl;
    }

    public void setReqUrl(String reqUrl) {
      this.reqUrl = reqUrl;
    }

    public Long getHeartbeat() {
      return heartbeat;
    }

    public void setHeartbeat(Long heartbeat) {
      this.heartbeat = heartbeat;
    }
  }
}
