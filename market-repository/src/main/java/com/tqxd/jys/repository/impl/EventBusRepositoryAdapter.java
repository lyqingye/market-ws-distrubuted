package com.tqxd.jys.repository.impl;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.RepositoryOpenApi;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Set;

/**
 * 消息总线仓库适配器
 *
 * @author lyqingye
 */
public class EventBusRepositoryAdapter implements KLineRepository{

  private RepositoryOpenApi openApi;

  public void open(Vertx vertx, JsonObject config, Handler<AsyncResult<Void>> handler) {
    openApi = RepositoryOpenApi.createProxy(vertx);
  }

  @Override
  public void listSymbols(Handler<AsyncResult<Set<String>>> handler) {
    openApi.listKlineKeys(handler);
  }

  @Override
  public void loadSnapshot(String symbol, Period period, Handler<AsyncResult<KlineSnapshot>> handler) {
    openApi.getKlineSnapshot(symbol, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(Json.decodeValue(ar.result(),KlineSnapshot.class)));
      }else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }
}
