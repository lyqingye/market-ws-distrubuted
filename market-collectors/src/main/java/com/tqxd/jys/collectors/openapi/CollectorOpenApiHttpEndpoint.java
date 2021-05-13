package com.tqxd.jys.collectors.openapi;

import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.openapi.CollectorOpenApi;
import com.tqxd.jys.openapi.payload.CollectorStatusDto;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 收集器http服务器
 *
 * @author lyqingye
 */
public class CollectorOpenApiHttpEndpoint extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(CollectorOpenApiHttpEndpoint.class);
  private static final String CFG_HTTP_HOST = "host";
  private static final String CFG_HTTP_PORT = "port";
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final int DEFAULT_PORT = 8080;
  private String host;
  private Integer port;
  private HttpServer httpServer;
  private CollectorOpenApi service;

  public CollectorOpenApiHttpEndpoint(CollectorOpenApi service) {
    this.service = Objects.requireNonNull(service);
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    host = VertxUtil.jsonGetValue(config(), CFG_HTTP_HOST, String.class);
    port = VertxUtil.jsonGetValue(config(), CFG_HTTP_PORT, Integer.class);
    if (host == null) {
      host = DEFAULT_HOST;
    }
    if (port == null) {
      port = DEFAULT_PORT;
    }
    Router router = Router.router(vertx);
    initRouterHandler(router);
    vertx.createHttpServer()
        .requestHandler(router)
        .exceptionHandler(Throwable::printStackTrace)
        .listen(port, host)
        .onComplete(ar -> {
          if (ar.succeeded()) {
            httpServer = ar.result();
            log.info("open api http endpoint listen on the http://{}:{}", host, port);
            startPromise.complete();
          } else {
            startPromise.fail(ar.cause());
          }
        });
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    if (httpServer != null) {
      httpServer.close(stopPromise);
    }
  }

  private void initRouterHandler(Router router) {
    router.route().failureHandler(ctx -> ctx.response().putHeader("Content-type", "application/json; charset=UTF-8").end(R.fail("系统挂了哦!")));
    router.get("/api/market/collectors/list").handler(ctx -> genericHandler(ctx, this::listCollectors));
    router.put("/api/market/collector/:collectorName/subscribe/:dataType/:symbol").handler(ctx -> genericHandler(ctx, this::subscribeSymbol));
    router.put("/api/market/collector/:collectorName/unSubscribe/:dataType/:symbol").handler(ctx -> genericHandler(ctx, this::unsubscribeSymbol));
    router.put("/api/market/collector/:collectorName/start").handler(ctx -> genericHandler(ctx, this::startCollector));
    router.put("/api/market/collector/:collectorName/stop").handler(ctx -> genericHandler(ctx, this::stopCollector));
  }

  private Future<List<CollectorStatusDto>> listCollectors(RoutingContext ctx) {
    Promise<List<CollectorStatusDto>> promise = Promise.promise();
    service.listCollector(promise);
    return promise.future();
  }

  private Future<Void> startCollector(RoutingContext ctx) {
    Promise<Void> promise = Promise.promise();
    service.startCollector(ctx.pathParam("collectorName"), promise);
    return promise.future();
  }

  private Future<Void> stopCollector(RoutingContext ctx) {
    Promise<Void> promise = Promise.promise();
    service.stopCollector(ctx.pathParam("collectorName"), promise);
    return promise.future();
  }

  private Future<Void> subscribeSymbol(RoutingContext ctx) {
    DataType dataType = DataType.valueOfName(ctx.pathParam("dataType"));
    if (dataType == null) {
      return Future.failedFuture("invalid data type");
    } else {
      Promise<Void> promise = Promise.promise();
      service.subscribe(ctx.pathParam("collectorName"), dataType, ctx.pathParam("symbol"), promise);
      return promise.future();
    }
  }

  private Future<Void> unsubscribeSymbol(RoutingContext ctx) {
    DataType dataType = DataType.valueOfName(ctx.pathParam("dataType"));
    if (dataType == null) {
      return Future.failedFuture("invalid data type");
    } else {
      Promise<Void> promise = Promise.promise();
      service.unsubscribe(ctx.pathParam("collectorName"), dataType, ctx.pathParam("symbol"), promise);
      return promise.future();
    }
  }

  <T> void genericHandler(RoutingContext ctx, Function<RoutingContext, Future<T>> handler) {
    handler.apply(ctx)
        .onSuccess(rs -> ctx.response()
            .putHeader("Content-type", "application/json;charset=UTF-8").end(R.success(rs)))
        .onFailure(throwable -> {
          throwable.printStackTrace();
          ctx.response()
              .putHeader("Content-type", "application/json;charset=UTF-8").end(R.fail(throwable));
        });
  }
}
