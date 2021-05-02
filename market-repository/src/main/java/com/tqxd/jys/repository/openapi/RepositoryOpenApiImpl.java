package com.tqxd.jys.repository.openapi;

import com.tqxd.jys.openapi.RepositoryOpenApi;
import com.tqxd.jys.openapi.ServiceAddress;
import com.tqxd.jys.repository.KlineRepositoryImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

import java.util.Objects;
import java.util.Set;

/**
 * 持久化开放API实现
 *
 * @author lyqingye
 */
public class RepositoryOpenApiImpl implements RepositoryOpenApi {
  private KlineRepositoryImpl repository;
  private Vertx vertx;
  private ServiceBinder serviceBinder;
  private MessageConsumer<JsonObject> serviceConsumer;

  private RepositoryOpenApiImpl() {
  }

  public RepositoryOpenApiImpl(Vertx vertx, KlineRepositoryImpl repository) {
    this.vertx = Objects.requireNonNull(vertx);
    this.repository = Objects.requireNonNull(repository);
  }

  public static RepositoryOpenApi init(Vertx vertx, KlineRepositoryImpl repository) {
    RepositoryOpenApiImpl api = new RepositoryOpenApiImpl(vertx, repository);
    api.serviceBinder = new ServiceBinder(vertx).setAddress(ServiceAddress.REPOSITORY.name());
    if (vertx.isClustered()) {
      api.serviceConsumer = api.serviceBinder.register(RepositoryOpenApi.class, api);
    } else {
      api.serviceConsumer = api.serviceBinder.registerLocal(RepositoryOpenApi.class, api);
    }
    return api;
  }

  public void unInit() {
    serviceBinder.unregister(serviceConsumer);
  }

  @Override
  public void listKlineKeys(Handler<AsyncResult<Set<String>>> handler) {
    repository.listKlineKeys(handler);
  }

  @Override
  public void getKlineSnapshot(String symbol, Handler<AsyncResult<String>> handler) {
    repository.getKlineSnapshot(symbol, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(Json.encode(ar.result())));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }
}
