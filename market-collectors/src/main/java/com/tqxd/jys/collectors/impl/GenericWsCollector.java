package com.tqxd.jys.collectors.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tqxd.jys.constance.DataType;
import io.vertx.core.Future;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author yjt
 * @since 2020/10/10 上午9:54
 */
public abstract class GenericWsCollector extends AbstractVerticle implements Collector {
  public static final String HTTP_CLIENT_OPTIONS_PARAM = "http_client_options_param";
  public static final String WS_REQUEST_PATH_PARAM = "ws_request_path_param";
  public static final String IDLE_TIME_OUT = "idle_time_out";

  static final ScheduledExecutorService idleCheckerExecutor;

  private static final int RETRY_MAX_COUNT = 255;

  Logger log = LoggerFactory.getLogger(GenericWsCollector.class);
  private Map<DataType, List<String>> subscribed = new HashMap<>(16);
  private DataReceiver[] receivers = new DataReceiver[16];
  private int numOfReceives = 0;
  private volatile boolean isRunning;
  private volatile WebSocket webSocket;
  private long lastReceiveTimestamp;
  private long idleTime;
  private long checkTime = TimeUnit.SECONDS.toMillis(5);
  private long lastCheckTime = 0;
  private volatile boolean restarting = false;
  private volatile boolean idleCheckEnable = true;

  static {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("collector-idle-check-thread%d")
        .setUncaughtExceptionHandler(((t, e) -> e.printStackTrace()))
        .build();
    idleCheckerExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  private int retryCount = 0;

  @Override
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    if (isRunning()) {
      startPromise.fail("collector is running!");
    } else {
      idleTime = config().getLong(IDLE_TIME_OUT, TimeUnit.SECONDS.toMillis(5));
      HttpClientOptions httpClientOptions = null;
      if (config().containsKey(HTTP_CLIENT_OPTIONS_PARAM)) {
        httpClientOptions = (HttpClientOptions) config().getValue(HTTP_CLIENT_OPTIONS_PARAM);
      }
      String requestPath = config().getString(WS_REQUEST_PATH_PARAM);
      HttpClient httpClient;
      if (httpClientOptions != null) {
        httpClient = vertx.createHttpClient(httpClientOptions);
      } else {
        httpClient = vertx.createHttpClient();
      }
      httpClient.webSocket(requestPath).onComplete(ar -> {
        if (ar.succeeded()) {
          retryCount = 0;
          isRunning = true;
          webSocket = ar.result();
          webSocket.frameHandler(frame -> {
            try {
              onFrame(webSocket, frame);
            } catch (Exception e) {
              log.error("process message fail! cause by {} collector!", this.name());
              e.printStackTrace();
            }
          });
          webSocket.closeHandler(none -> {
            stopIdleChecker();
            log.warn("[Collectors]: collector {} connection closed try to restart!", this.name());
            retry();
          });
          webSocket.exceptionHandler(throwable -> {
            stopIdleChecker();
            log.info("[Collectors]: collector {} catch exception try to restart!", this.name());
            throwable.printStackTrace();
            retry();
          });
          if (idleCheckEnable) {
            startIdleChecker();
          }
          startPromise.complete();
        } else {
          startPromise.fail(ar.cause());
        }
      });
    }
  }

  private synchronized void retry() {
    if (retryCount++ >= RETRY_MAX_COUNT) {
      log.error("[collectors]: collector {} retryCount == 255!", this.name());
    } else {
      log.info("[collectors]: collector {} retry: {}", this.name(), retryCount);
      restart()
          .onComplete(v -> {
            if (v.failed()) {
              v.cause().printStackTrace();
            } else {
              log.info("[collectors]: collector {} retry success!", this.name());
              retryCount = 0;
            }
          });
    }
  }

  @Override
  public synchronized void stop(Promise<Void> stopPromise) throws Exception {
    if (!this.isRunning()) {
      stopPromise.fail("collector running yet!");
    } else {
      log.info("[collectors]: collector {} stopping, try to close client!", this.name());
      webSocket.close((short) 0, "collector call the stop!", ar -> {
        // force set state is close
        this.isRunning = false;
        stopIdleChecker();
        if (ar.succeeded()) {
          webSocket = null;
        } else {
          log.info("collector: {} stop!", this.name());
          log.warn("close the socket fail! ignore this error!");
          ar.cause().printStackTrace();
        }
        stopPromise.complete();
      });
    }
  }

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

  /**
   * 是否正在收集
   *
   * @return 是否正在收集
   */
  @Override
  public boolean isRunning() {
    return this.isRunning;
  }

  public void writeText(String text) {
    if (isRunning() && webSocket != null) {
      webSocket.writeTextMessage(text);
    }
  }

  public void writeBinary(Buffer buffer) {
    if (isRunning() && webSocket != null) {
      webSocket.writeBinaryMessage(buffer);
    }
  }

  public void writePong (Buffer buffer) {
    if (isRunning() && webSocket != null){
      webSocket.writePong(buffer);
    }
  }

  public void onFrame(WebSocket client, WebSocketFrame frame) {
    refreshLastReceiveTime();
  }

  /**
   * 启动空闲检测
   */
  public void startIdleChecker() {
    idleCheckEnable = true;
    lastCheckTime = System.currentTimeMillis();
    idleCheckerExecutor.scheduleWithFixedDelay(() -> {
      if (!isRunning() || !idleCheckEnable || restarting || (System.currentTimeMillis() - lastCheckTime) < checkTime) {
        return;
      }
      lastCheckTime = System.currentTimeMillis();
      if ((System.currentTimeMillis() - lastReceiveTimestamp) >= idleTime) {
        restarting = true;
        CompletableFuture<Void> cf = new CompletableFuture<>();
        log.info("[Collectors]: collector {} idle detected, try to restart! lastReceiveTime: {} now: {}", this.name(), new Date(lastReceiveTimestamp), new Date());
        restart(ar -> {
          if (ar.succeeded()) {
            cf.complete(null);
          } else {
            cf.completeExceptionally(ar.cause());
          }
        });
        try {
          cf.get();
          log.info("[Collectors]: collector {} restart success!", this.name());
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
        restarting = false;
        lastCheckTime = System.currentTimeMillis();
      }
    }, 5, 1, TimeUnit.SECONDS);
  }

  /**
   * 刷新上一次收到消息的时间, 用于空闲链路检测
   */
  public void refreshLastReceiveTime() {
    lastReceiveTimestamp = System.currentTimeMillis();
  }

  /**
   * 停止空闲检测
   */
  public void stopIdleChecker() {
    idleCheckEnable = false;
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
