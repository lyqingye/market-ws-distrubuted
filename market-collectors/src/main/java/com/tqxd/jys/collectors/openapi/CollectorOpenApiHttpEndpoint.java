package com.tqxd.jys.collectors.openapi;

import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.openapi.CollectorOpenApi;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

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
    router.get("/api/market/collectors/list").handler(this::listCollectors);
    router.put("/api/market/collector/:collectorName/subscribe/:dataType/:symbol").handler(this::subscribeSymbol);
    router.put("/api/market/collector/:collectorName/unSubscribe/:dataType/:symbol").handler(this::unsubscribeSymbol);
    router.put("/api/market/collector/:collectorName/start").handler(this::startCollector);
    router.put("/api/market/collector/:collectorName/stop").handler(this::stopCollector);
  }

  private void listCollectors(RoutingContext ctx) {
    service.listCollector(ar -> {
      if (ar.succeeded()) {
        ctx.end(R.success(ar.result()));
      } else {
        ar.cause().printStackTrace();
        ctx.end(R.fail(ar.cause()));
      }
    });
  }

  private void startCollector(RoutingContext ctx) {
    service.startCollector(ctx.pathParam("collectorName"), ar -> {
      if (ar.succeeded()) {
        ctx.end(R.success());
      } else {
        ar.cause().printStackTrace();
        ctx.end(R.fail(ar.cause()));
      }
    });
  }

  private void stopCollector(RoutingContext ctx) {
    service.stopCollector(ctx.pathParam("collectorName"), ar -> {
      if (ar.succeeded()) {
        ctx.end(R.success());
      } else {
        ar.cause().printStackTrace();
        ctx.end(R.fail(ar.cause()));
      }
    });
  }

  private void subscribeSymbol(RoutingContext ctx) {
    DataType dataType = DataType.valueOfName(ctx.pathParam("dataType"));
    if (dataType == null) {
      ctx.end(R.fail("invalid data type"));
    } else {
      service.subscribe(ctx.pathParam("collectorName"), dataType, ctx.pathParam("symbol"), ar -> {
        if (ar.succeeded()) {
          ctx.end(R.success());
        } else {
          ar.cause().printStackTrace();
          ctx.end(R.fail(ar.cause()));
        }
      });
    }

  }

  private void unsubscribeSymbol(RoutingContext ctx) {
    DataType dataType = DataType.valueOfName(ctx.pathParam("dataType"));
    if (dataType == null) {
      ctx.end(R.fail("invalid data type"));
    } else {
      service.unsubscribe(ctx.pathParam("collectorName"), dataType, ctx.pathParam("symbol"), ar -> {
        if (ar.succeeded()) {
          ctx.end(R.success());
        } else {
          ar.cause().printStackTrace();
          ctx.end(R.fail(ar.cause()));
        }
      });
    }
  }
}
