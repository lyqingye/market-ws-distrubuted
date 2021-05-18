package com.tqxd.jys.repository;

import com.tqxd.jys.bootstrap.Bootstrap;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.repository.faced.EventBusRepositoryFaced;
import com.tqxd.jys.repository.impl.CacheableKLineRepositoryProxy;
import com.tqxd.jys.repository.impl.RedisKLineRepository;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.sync.MBKLineRepositoryAppendedSyncer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author lyqingye
 */
@SuppressWarnings("unchecked")
public class RepositoryApplication extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(RepositoryApplication.class);
  private KLineRepository kLineRepository;
  private EventBusRepositoryFaced ebRepositoryFaced;
  private String msgBusRegistryId;

  public static void main(String[] args) {
    Bootstrap.run(new RepositoryApplication(), new DeploymentOptions().setWorker(true));
  }


  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    kLineRepository = new CacheableKLineRepositoryProxy(new RedisKLineRepository());
    ebRepositoryFaced = new EventBusRepositoryFaced(kLineRepository);
    MBKLineRepositoryAppendedSyncer syncer = new MBKLineRepositoryAppendedSyncer();
    kLineRepository
      .open(vertx, config())
        // 将k线数据注册到k线仓库同步器
        .compose(none -> MessageBusFactory.bus().subscribe(Topic.KLINE_TICK_TOPIC, syncer))
        // 注册 eventbus open api
        .compose(registryId -> {
          this.msgBusRegistryId = registryId;
          log.info("[RepositoryApplication]: register message bus success! registryId: {}", msgBusRegistryId);
          ebRepositoryFaced.register(vertx);
          log.info("[RepositoryApplication]: register eventbus faced success!");
          // 同步数据到仓库
          return syncer.syncAppendTo(kLineRepository);
        })
        .onSuccess(none -> {
          Router router = Router.router(vertx);
          router.get("/market/history/kline")
              .handler(ctx -> {
                final String period = ctx.request().getParam("period");
                int size = Integer.parseInt(ctx.request().getParam("size"));
                final String symbol = ctx.request().getParam("symbol");
                long lastDay = (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1)) / 1000;
                long now = System.currentTimeMillis() / 1000;
                kLineRepository.query(symbol, Period.valueOfSymbol(period), lastDay, now, ar -> {
                  if (ar.succeeded()) {
                    JsonObject result = new JsonObject();
                    result.put("ch", String.format("market.%s.kline.%s", symbol, Objects.requireNonNull(Period.valueOfSymbol(period))));
                    result.put("status", "ok");
                    result.put("data", ar.result());
                    ctx.response().putHeader("content-type", "application/json")
                        .putHeader("Access-Control-Allow-Origin", "*")
                        .putHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT")
                        .putHeader("Access-Control-Allow-Credentials", "true")
                        .end(Json.encodePrettily(result));
                  } else {
                    ar.cause().printStackTrace();
                    ctx.end(ar.cause().getLocalizedMessage());
                  }
                });
              });
          router.get("/v1/common/symbols")
              .handler(ctx -> {
                ctx.response().putHeader("content-type", "application/json")
                    .putHeader("Access-Control-Allow-Origin", "*")
                    .putHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT")
                    .putHeader("Access-Control-Allow-Credentials", "true")
                    .end("{\n" +
                        "    \"status\": \"ok\",\n" +
                        "    \"data\": [\n" +
                        "        {\n" +
                        "            \"base-currency\": \"btc\",\n" +
                        "            \"quote-currency\": \"usdt\",\n" +
                        "            \"price-precision\": 2,\n" +
                        "            \"amount-precision\": 6,\n" +
                        "            \"symbol-partition\": \"main\",\n" +
                        "            \"symbol\": \"btcusdt\",\n" +
                        "            \"state\": \"online\",\n" +
                        "            \"value-precision\": 8,\n" +
                        "            \"min-order-amt\": 0.0001,\n" +
                        "            \"max-order-amt\": 1000,\n" +
                        "            \"min-order-value\": 5,\n" +
                        "            \"limit-order-min-order-amt\": 0.0001,\n" +
                        "            \"limit-order-max-order-amt\": 1000,\n" +
                        "            \"sell-market-min-order-amt\": 0.0001,\n" +
                        "            \"sell-market-max-order-amt\": 100,\n" +
                        "            \"buy-market-max-order-value\": 1000000,\n" +
                        "            \"leverage-ratio\": 5,\n" +
                        "            \"super-margin-leverage-ratio\": 3,\n" +
                        "            \"funding-leverage-ratio\": 3,\n" +
                        "            \"api-trading\": \"enabled\"\n" +
                        "        }\n" +
                        "    ]\n" +
                        "}");
              });

          router.route().handler(CorsHandler.create("*")
              .allowedMethod(io.vertx.core.http.HttpMethod.GET)
              .allowedMethod(HttpMethod.PUT)
              .allowedMethod(io.vertx.core.http.HttpMethod.POST)
              .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
              .allowCredentials(true)
              .allowedHeader("Access-Control-Request-Method")
              .allowedHeader("Access-Control-Allow-Credentials")
              .allowedHeader("Access-Control-Allow-Origin")
              .allowedHeader("Access-Control-Allow-Headers")
              .allowedHeader("Content-Type"));
          router.route().handler(BodyHandler.create());
          vertx.createHttpServer()
              .requestHandler(router)
              .exceptionHandler(Throwable::printStackTrace)
              .listen(8889, "0.0.0.0")
              .onComplete(ar -> {
                if (ar.succeeded()) {
//                  startPromise.complete();
                } else {
                  ar.cause().printStackTrace();
//                  startPromise.fail(ar.cause());
                }
              });
        })

        .onFailure(Throwable::printStackTrace)
      .onComplete(startPromise);


  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    ebRepositoryFaced.unregister();
    MessageBusFactory.bus()
      .unSubscribe(Topic.KLINE_TICK_TOPIC,msgBusRegistryId, ar -> {
        if (ar.succeeded()) {
          kLineRepository.close(stopPromise);
        }else {
          stopPromise.fail(ar.cause());
        }
      });
  }
}
