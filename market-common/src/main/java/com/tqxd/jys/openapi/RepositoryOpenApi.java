package com.tqxd.jys.openapi;

import com.tqxd.jys.constance.Period;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;

import java.util.Set;

/**
 * 持久化仓库OpenApi
 *
 * @author lyqingye
 */
@ProxyGen
public interface RepositoryOpenApi {

  static RepositoryOpenApi createProxy(Vertx vertx) {
    return new RepositoryOpenApiVertxEBProxy(vertx, ServiceAddress.REPOSITORY.name(), new DeliveryOptions().setSendTimeout(5000));
  }

  void listKlineKeys(Handler<AsyncResult<Set<String>>> handler);

  void getKlineSnapshot(String symbol, Period period, Handler<AsyncResult<String>> handler);
}
