package com.tqxd.jys.collectors.impl.tqxd;


import com.tqxd.jys.collectors.impl.BasicCollector;
import com.tqxd.jys.collectors.impl.Collector;
import com.tqxd.jys.collectors.impl.DataReceiver;
import com.tqxd.jys.collectors.impl.tqxd.helper.TqxdDataConvert;
import com.tqxd.jys.collectors.impl.tqxd.match.TqxdDepthCollector;
import com.tqxd.jys.collectors.impl.tqxd.match.TqxdKlineCollector;
import com.tqxd.jys.collectors.impl.tqxd.match.TqxdTradeDetailCollector;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.Period;
import io.vertx.core.*;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tqxd.jys.collectors.impl.GenericWsCollector.*;

/**
 * @author ex
 */
@SuppressWarnings("Duplicates")
public class TqxdCollector extends BasicCollector {
  public static final String SYMBOL_CONFIG = "symbol";
  private static final Logger log = LoggerFactory.getLogger(TqxdCollector.class);
  private volatile boolean isRunning = false;

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
    HttpClientOptions httpClientOptions = new HttpClientOptions().setDefaultHost(config().getString("host"));
    config().put(HTTP_CLIENT_OPTIONS_PARAM, httpClientOptions);
    config().put(WS_REQUEST_PATH_PARAM, config().getString("path"));
    config().put(IDLE_TIME_OUT, -1);

    List<Future> futures = new ArrayList<>();
    super.listSubscribedInfo().forEach(((collectDataType, symbols) -> {
      if (symbols != null) {
        for (String symbol : symbols) {
          futures.add(this.subscribe(collectDataType, symbol));
        }
      }
    }));
    CompositeFuture.any(futures)
        .onSuccess(ar -> {
          startPromise.complete();
          log.info("[tqxd]: start success!");
          isRunning = true;
        })
        .onFailure(startPromise::fail);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void stop(Promise<Void> stopPromise) throws Exception {
    List<Future> futures = new ArrayList<>();
    super.listSubscribedInfo().forEach(((collectDataType, symbols) -> {
      if (symbols != null) {
        for (String symbol : symbols) {
          futures.add(this.unSubscribe(collectDataType, symbol));
        }
      }
    }));
    CompositeFuture.any(futures)
        .onComplete(ar -> {
          if (ar.succeeded()) {
            log.info("[tqxd]: stop success");
            depthCollectorVerticleMap.clear();
            isRunning = false;
            stopPromise.complete();
          } else {
            stopPromise.fail(ar.cause());
          }
        });
  }

  /**
   * 订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否订阅成功
   */
  @Override
  @SuppressWarnings("rawtypes")
  public void subscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.subscribe(dataType, symbol, promise);
    promise.future()
        .onSuccess(none -> {
          switch (dataType) {
            case KLINE: {
              List<Future> futures = new ArrayList<>();
              for (Period period : Period.values()) {
                Promise<Void> prom = Promise.promise();
                deployCollector(dataType, symbol, period, new TqxdKlineCollector(this.createConfig(symbol, period)), prom);
                futures.add(prom.future());
              }
              CompositeFuture.all(futures).onComplete(ar -> {
                if (ar.succeeded()) {
                  handler.handle(Future.succeededFuture());
                } else {
                  handler.handle(Future.failedFuture(ar.cause()));
                }
              });
              break;
            }
            case DEPTH: {
              deployCollector(dataType, symbol, null, new TqxdDepthCollector(this.createConfig(symbol, null)), handler);
              break;
            }
            case TRADE_DETAIL: {
              deployCollector(dataType, symbol, null, new TqxdTradeDetailCollector(this.createConfig(symbol, null)), handler);
              break;
            }
          }
          handler.handle(Future.failedFuture("[tqxd]: unknown data type for: " + dataType));
        })
        .onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
  }

  private void deployCollector(DataType dataType, String symbol, Period period, Collector collector, Handler<AsyncResult<Void>> handler) {
    final TqxdCollector that = this;
    collector.addDataReceiver(new DataReceiver() {
      @Override
      public void onReceive(Collector from, DataType dataType, JsonObject obj) {
        that.unParkReceives(dataType, TqxdDataConvert.trade(obj, symbol));
      }
    });
    vertx.deployVerticle(collector, ar -> {
      if (ar.succeeded()) {
        String key = dataType + symbol;
        if (period != null) {
          key += period;
        }
        depthCollectorVerticleMap.put(key, ar.result());
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void unDeployCollector(DataType dataType, String symbol, Period period, Handler<AsyncResult<Void>> handler) {
    String key = dataType + symbol;
    if (period != null) {
      key += symbol;
    }
    String deploymentID = depthCollectorVerticleMap.get(key);
    if (deploymentID != null) {
      vertx.undeploy(deploymentID, handler);
    } else {
      handler.handle(Future.succeededFuture());
    }
  }

  private JsonObject createConfig(String symbol, Period period) {
    return new JsonObject()
        .put(HTTP_CLIENT_OPTIONS_PARAM, config().getValue(HTTP_CLIENT_OPTIONS_PARAM))
        .put(WS_REQUEST_PATH_PARAM, config().getValue(WS_REQUEST_PATH_PARAM))
        .put(SYMBOL_CONFIG, symbol)
        .put(TqxdKlineCollector.PERIOD_CONFIG, period)
        .put(IDLE_TIME_OUT, config().getValue(IDLE_TIME_OUT));
  }

  /**
   * 取消订阅一个交易对
   *
   * @param dataType 数据收集类型
   * @param symbol   交易对
   * @return 是否取消订阅成功
   */
  @Override
  @SuppressWarnings("rawtypes")
  public void unSubscribe(DataType dataType, String symbol, Handler<AsyncResult<Void>> handler) {
    Promise<Void> promise = Promise.promise();
    super.unSubscribe(dataType, symbol, promise);
    promise.future()
        .onSuccess(none -> {
          switch (dataType) {
            case KLINE: {
              List<Future> futures = new ArrayList<>();
              for (Period period : Period.values()) {
                Promise<Void> prom = Promise.promise();
                unDeployCollector(dataType, symbol, period, prom);
                futures.add(prom.future());
              }
              CompositeFuture.all(futures).onComplete(ar -> {
                if (ar.succeeded()) {
                  handler.handle(Future.succeededFuture());
                } else {
                  handler.handle(Future.failedFuture(ar.cause()));
                }
              });
              break;
            }
            case TRADE_DETAIL:
            case DEPTH: {
              unDeployCollector(dataType, symbol, null, handler);
              break;
            }
          }
          handler.handle(Future.failedFuture("[tqxd]: unknown data type for: " + dataType));
        })
        .onFailure(throwable -> {
          handler.handle(Future.failedFuture(throwable));
        });
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public String name() {
    return TqxdCollector.class.getName();
  }

  /**
   * 描述一个收集器
   *
   * @return 收集器描述
   */
  @Override
  public String desc() {
    return "天启旭达数据收集器";
  }


}
