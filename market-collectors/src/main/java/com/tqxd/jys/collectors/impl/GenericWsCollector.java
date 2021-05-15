package com.tqxd.jys.collectors.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yjt
 * @since 2020/10/10 上午9:54
 */
public abstract class GenericWsCollector extends BasicCollector {
  public static final String HTTP_CLIENT_OPTIONS_PARAM = "http_client_options_param";
  public static final String WS_REQUEST_PATH_PARAM = "ws_request_path_param";
  public static final String IDLE_TIME_OUT = "idle_time_out";

  private static final ScheduledExecutorService idleCheckerExecutor;
  private Logger log = LoggerFactory.getLogger(GenericWsCollector.class);
  private static final int RETRY_MAX_COUNT = 255;
  private volatile boolean isRunning;
  private volatile WebSocket webSocket;
  private long lastReceiveTimestamp;
  private long idleTime;
  private long checkTime = TimeUnit.SECONDS.toMillis(5);
  private long lastCheckTime = 0;
  private volatile boolean restarting = false;
  private volatile boolean stopping = false;
  private volatile boolean idleCheckEnable = true;
  private ReentrantLock startLock = new ReentrantLock();
  private ReentrantLock stopLock = new ReentrantLock();
  private ReentrantLock restartLock = new ReentrantLock();
  private AtomicInteger retryCount = new AtomicInteger(0);
  static {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("collector-idle-check-thread%d")
        .setUncaughtExceptionHandler(((t, e) -> e.printStackTrace()))
        .build();
    idleCheckerExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  @Override
  public synchronized void start(Promise<Void> startPromise) throws Exception {
    if (isRunning()) {
      startPromise.fail("collector is running!");
    } else {
      startLock.lock();
      try {
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
          startLock.unlock();
          if (ar.succeeded()) {
            retryCount.set(0);
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
              log.warn("[Collectors]: collector {} connection closed try to restart!", this.name());
              stopIdleChecker();
//              retry();
            });
            webSocket.exceptionHandler(throwable -> {
              // 币安bug，会发送一个 错误的 CloseFrame 实际上并没有停止推送
              if (throwable instanceof CorruptedWebSocketFrameException) {
                log.info("[Collectors]: collector {} catch exception try to restart!", this.name());
              } else {
//                stopIdleChecker();
//                retry();
              }
              throwable.printStackTrace();
            });
            if (idleCheckEnable && idleTime != -1) {
              startIdleChecker();
            }
            startPromise.complete();
          } else {
            startPromise.fail(ar.cause());
          }
        });
      } catch (Exception ex) {
        startLock.unlock();
      }
    }
  }

  private synchronized void retry() {
    if (stopping) {
      return;
    }
    if (retryCount.getAndIncrement() >= RETRY_MAX_COUNT) {
      log.error("[collectors]: collector {} retryCount == 255!", this.name());
    } else {
      restartLock.lock();
      log.info("[collectors]: collector {} retry: {}", this.name(), retryCount);
      restart()
          .onComplete(v -> {
            restartLock.unlock();
            if (v.failed()) {
              v.cause().printStackTrace();
            } else {
              log.info("[collectors]: collector {} retry success!", this.name());
              retryCount.set(0);
            }
          });
    }
  }

  @Override
  public synchronized void stop(Promise<Void> stopPromise) throws Exception {
    if (!this.isRunning() && stopping) {
      stopPromise.fail("collector running yet! or stopping");
    } else {
      stopLock.lock();
      stopping = true;
      try {
        log.info("[collectors]: collector {} stopping, try to close client!", this.name());
        webSocket.close((short) 66666, "collector call the stop!", ar -> {
          stopLock.unlock();
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
          this.stopping = false;
          stopPromise.complete();
        });
      } catch (Exception ex) {
        stopLock.unlock();
      }
    }
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
