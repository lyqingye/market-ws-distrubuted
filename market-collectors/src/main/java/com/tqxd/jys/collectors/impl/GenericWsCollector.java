package com.tqxd.jys.collectors.impl;

import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yjt
 * @since 2020/10/10 上午9:54
 */
public abstract class GenericWsCollector extends BasicCollector {
  public static final String HTTP_CLIENT_OPTIONS_PARAM = "http_client_options_param";
  public static final String WS_REQUEST_PATH_PARAM = "ws_request_path_param";
  public static final String IDLE_TIME_OUT = "idle_time_out";

  private static final ScheduledExecutorService idleCheckerExecutor;
  private static final int RETRY_MAX_COUNT = 255;

  private static final Logger log = LoggerFactory.getLogger(GenericWsCollector.class);

  static {
    idleCheckerExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(false);
        thread.setName("collector-idle-check-thread");
        thread.setUncaughtExceptionHandler(((t, e) -> e.printStackTrace()));
        return thread;
      }
    });
  }

  private final long checkTime = TimeUnit.SECONDS.toMillis(5);
  private volatile boolean isRunning;
  private volatile WebSocket webSocket;
  private long idleTime;
  private volatile long lastReceiveTimestamp;
  private volatile long lastCheckTime = 0;
  private volatile boolean restarting = false;
  private volatile boolean stopping = false;
  private volatile boolean idleCheckEnable = true;
  private AtomicInteger retryCount = new AtomicInteger(0);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    getStartLock()
      .onSuccess(lock -> {
        if (isRunning()) {
          lock.release();
          startPromise.fail("collector is running!");
        } else {
          idleTime = config().getLong(IDLE_TIME_OUT, TimeUnit.SECONDS.toMillis(5));
          HttpClientOptions httpClientOptions = null;
          if (config().containsKey(HTTP_CLIENT_OPTIONS_PARAM)) {
            httpClientOptions = (HttpClientOptions) config().getValue(HTTP_CLIENT_OPTIONS_PARAM);
          } else {
            httpClientOptions = new HttpClientOptions();
          }
          httpClientOptions.setConnectTimeout(Math.toIntExact(TimeUnit.SECONDS.toMillis(5)));
          String requestPath = config().getString(WS_REQUEST_PATH_PARAM);
          HttpClient httpClient;
          httpClient = vertx.createHttpClient(httpClientOptions);
          httpClient.webSocket(requestPath).onComplete(ar -> {
            if (ar.succeeded()) {
              retryCount.set(0);
              isRunning = true;
              webSocket = ar.result();
              registerWebsocketHandler(webSocket);
              if (idleCheckEnable && idleTime != -1) {
                startIdleChecker();
              }
              log.info("collector: {} start success!", this.name());
              startPromise.complete();
            } else {
              log.error("[collectors]: collector: {} start fail! cause by: {}", this.name(), ar.cause().getMessage());
              startPromise.fail(ar.cause());
            }
            lock.release();
          });
        }
      })
      .onFailure(startPromise::fail);
  }

  private void registerWebsocketHandler(WebSocket websocket) {
    webSocket.frameHandler(frame -> {
      try {
        onFrame(webSocket, frame);
      } catch (Exception e) {
        log.error("process message fail! cause by {} collector!", this.name());
        e.printStackTrace();
      }
    });
    webSocket.closeHandler(none -> {
      log.warn("[Collectors]: collector {} connection closed try to restart!", this.name());
      stopIdleChecker();
      retry();
    });
    webSocket.exceptionHandler(throwable -> {
      // 币安bug，会发送一个 错误的 CloseFrame 实际上并没有停止推送
      if (throwable instanceof CorruptedWebSocketFrameException) {
        log.info("[Collectors]: collector {} catch close frame exception ", this.name());
      } else if (throwable instanceof VertxException && "Connection was closed".equals(throwable.getMessage())) {
        // do nothing
        // 如果为连接关闭异常，会直接通知到上面的 closeHandler，这边不做处理
      } else {
        stopIdleChecker();
        retry();
      }
      throwable.printStackTrace();
    });
  }

  private Future<Lock> getStartLock() {
    return vertx.sharedData().getLocalLock(deploymentID() + this.name());
  }

  private Future<Lock> getStopLock() {
    return vertx.sharedData().getLocalLock(deploymentID() + this.name());
  }

  private Future<Lock> getRestartLock() {
    return vertx.sharedData().getLocalLockWithTimeout(deploymentID() + this.name(), TimeUnit.SECONDS.toMillis(10));
  }

  private synchronized void retry() {
    getRestartLock()
      .onSuccess(lock -> {
        if (stopping) {
          lock.release();
          return;
        }
        if (retryCount.getAndIncrement() >= RETRY_MAX_COUNT) {
          log.error("[collectors]: collector {} retryCount == 255!", this.name());
        } else {
          log.info("[collectors]: collector {} retry: {}", this.name(), retryCount);
          restart()
            .onComplete(v -> {
              if (v.failed()) {
                log.info("[collectors]: collector {} restart fail! cause by: {}", this.name(), v.cause().getMessage());
                v.cause().printStackTrace();
              } else {
                log.info("[collectors]: collector {} retry success!", this.name());
                retryCount.set(0);
              }
              lock.release();
            });
        }
      });
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    getStopLock()
      .onSuccess(lock -> {
        if (!this.isRunning() || stopping || webSocket == null) {
          stopPromise.fail("collector running yet! or stopping");
          lock.release();
        } else {
          stopping = true;
          log.info("[collectors]: collector {} stopping, try to close client!", this.name());
          webSocket.close((short) 66666, "collector call the stop!", ar -> {
            // force set state is close
            this.isRunning = false;
            stopIdleChecker();
            if (ar.succeeded()) {
              log.info("[collectors]: collector {} stop success!", this.name());
              webSocket = null;
            } else {
              log.warn("[collectors]: collector close the socket fail! cause by: {}", ar.cause().getMessage());
              ar.cause().printStackTrace();
            }
            this.stopping = false;
            lock.release();
            stopPromise.complete();
          });
        }
      })
      .onFailure(stopPromise::fail);
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

  public void writePong(Buffer buffer) {
    if (isRunning() && webSocket != null) {
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
      if (!isRunning() || !idleCheckEnable || idleTime == -1 || restarting || (System.currentTimeMillis() - lastCheckTime) < checkTime) {
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
}
