package com.tqxd.jys.openapi;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;

import java.util.Set;

/**
 * 持久化仓库OpenApi
 *
 * @author lyqingye
 */
@ProxyGen
public interface RepositoryOpenApi {
  @GenIgnore
  static RepositoryOpenApi createProxy(Vertx vertx) {
    return new RepositoryOpenApiVertxEBProxy(vertx, ServiceAddress.REPOSITORY.name(), new DeliveryOptions().setSendTimeout(5000));
  }

  void listKlineKeys(Handler<AsyncResult<Set<JsonObject>>> handler);

  void getKlineSnapshot(String symbol, Handler<AsyncResult<JsonObject>> handler);
}
