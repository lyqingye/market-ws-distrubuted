package com.tqxd.jys.repository.faced;

import com.tqxd.jys.constance.Period;
import com.tqxd.jys.openapi.RepositoryOpenApi;
import com.tqxd.jys.openapi.ServiceAddress;
import com.tqxd.jys.repository.impl.KLineRepository;
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
 * 时间总线仓库门面
 *
 * @author lyqingye
 */
public class EventBusRepositoryFaced implements RepositoryOpenApi {
  private KLineRepository repository;
  private ServiceBinder serviceBinder;
  private MessageConsumer<JsonObject> serviceConsumer;

  public EventBusRepositoryFaced(KLineRepository repository) {
    this.repository = Objects.requireNonNull(repository);
  }

  public void register (Vertx vertx) {
    serviceBinder = new ServiceBinder(vertx).setAddress(ServiceAddress.REPOSITORY.name());
    if (vertx.isClustered()) {
      serviceConsumer = serviceBinder.register(RepositoryOpenApi.class, this);
    } else {
      serviceConsumer = serviceBinder.registerLocal(RepositoryOpenApi.class, this);
    }
  }

  public void unregister () {
    serviceBinder.unregister(serviceConsumer);
  }

  @Override
  public void listKlineKeys(Handler<AsyncResult<Set<String>>> handler) {
    repository.listSymbols(handler);
  }

  @Override
  public void getKlineSnapshot(String symbol, Handler<AsyncResult<String>> handler) {
    repository.loadSnapshot(symbol, Period._1_MIN,ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(Json.encode(ar.result())));
      }else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }
}
