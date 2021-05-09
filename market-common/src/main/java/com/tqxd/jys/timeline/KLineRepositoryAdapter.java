package com.tqxd.jys.timeline;

import com.tqxd.jys.constance.Period;
import com.tqxd.jys.openapi.RepositoryOpenApi;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Set;

/**
 * 消息总线仓库适配器
 *
 * @author lyqingye
 */
public class KLineRepositoryAdapter implements KLineRepository{

  private RepositoryOpenApi openApi;

  public void open(Vertx vertx, JsonObject config, Handler<AsyncResult<Void>> handler) {
    openApi = RepositoryOpenApi.createProxy(vertx);
    handler.handle(Future.succeededFuture());
  }

  @Override
  public void listSymbols(Handler<AsyncResult<Set<String>>> handler) {
    openApi.listKlineKeys(ar -> ar.map(v -> v.stream().map(JsonObject::toString)));
  }

  @Override
  public void loadSnapshot(String symbol, Period period, Handler<AsyncResult<KlineSnapshot>> handler) {
    openApi.getKlineSnapshot(symbol, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(ar.result().mapTo(KlineSnapshot.class)));
      }else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }
}
