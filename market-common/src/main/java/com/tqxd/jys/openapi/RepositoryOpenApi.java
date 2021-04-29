package com.tqxd.jys.openapi;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.Set;

/**
 * 持久化仓库OpenApi
 *
 * @author lyqingye
 */
@ProxyGen
public interface RepositoryOpenApi {

  static RepositoryOpenApi createProxy(Vertx vertx) {
    return new RepositoryOpenApiVertxEBProxy(vertx, ServiceAddress.REPOSITORY.name());
  }

  void listKlineKeys(Handler<AsyncResult<Set<String>>> handler);

  void getKlineSnapshot(String symbol, Handler<AsyncResult<String>> handler);
}
