package com.tqxd.jys.collectors;

import com.tqxd.jys.bootstrap.Bootstrap;
import com.tqxd.jys.collectors.openapi.CollectorOpenApiHttpEndpoint;
import com.tqxd.jys.collectors.openapi.CollectorOpenApiImpl;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.openapi.CollectorOpenApi;
import com.tqxd.jys.openapi.ServiceAddress;
import com.tqxd.jys.openapi.payload.CollectorStatusDto;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ServiceBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.tqxd.jys.utils.VertxUtil.jsonGetValue;
import static com.tqxd.jys.utils.VertxUtil.jsonListValue;

/**
 * @author yjt
 * @since 2020/10/10 下午3:45
 */
@SuppressWarnings("unchecked")
public class CollectorsApplication extends AbstractVerticle {
  private static final String ENABLED_COLLECTORS_CONFIG = "market.collectors.enabled";
  private static final String COLLECTOR_FORMAT = "market.collectors.%s";
  private static final String COLLECTOR_CLAZZ = "clazz";
  private static final String COLLECTOR_CONFIG = "config";
  private static final String COLLECTOR_SUBSCRIBE_KLINE = "subscribe.kline";
  private static final String COLLECTOR_SUBSCRIBE_DEPTH = "subscribe.depth";
  private static final String COLLECTOR_SUBSCRIBE_TRADE_DETAIL = "subscribe.trade.detail";
  private static final String OPEN_API_HTTP_ENDPOINT_CONFIG = "market.collectors.api.endpoint.http";
  private static final Logger log = LoggerFactory.getLogger(CollectorsApplication.class);

  /**
   * expose service
   */
  private ServiceBinder serviceBinder;

  /**
   * consumer
   */
  private MessageConsumer<JsonObject> serviceConsumer;

  /**
   * 开放服务
   */
  private CollectorOpenApiImpl openService;

  public CollectorsApplication() {
  }

  public static void main(String[] args) {
    Bootstrap.run(new CollectorsApplication(), new DeploymentOptions().setWorker(true));
  }

  @Override
  public void stop() throws Exception {
    serviceBinder.unregister(serviceConsumer);
    // 停止所有收集器
    if (openService != null) {
      openService.listCollector(cr -> {
        if (cr.succeeded()) {
          for (CollectorStatusDto collector : cr.result()) {
            openService.stopCollector(collector.getName(), stopRs -> {
              if (stopRs.failed()) {
                stopRs.cause().printStackTrace();
              }
            });
          }
        }
      });
    }
  }


  @Override
  @SuppressWarnings("rawtypes")
  public void start(Promise<Void> promise) throws Exception {
    // 暴露服务
    serviceBinder = new ServiceBinder(vertx).setAddress(ServiceAddress.COLLECTOR.name()).setTimeoutSeconds(5);
    openService = new CollectorOpenApiImpl(vertx, MessageBusFactory.bus());
    if (vertx.isClustered()) {
      serviceConsumer = serviceBinder.register(CollectorOpenApi.class, openService);
    } else {
      serviceConsumer = serviceBinder.registerLocal(CollectorOpenApi.class, openService);
    }
    JsonObject config = config();
    List<String> enabledCollectors = jsonListValue(config, ENABLED_COLLECTORS_CONFIG, String.class);
    List<Future> collectorsFutures = new ArrayList<>();
    for (String collectorKey : enabledCollectors) {
      JsonObject collectorJson = jsonGetValue(config, String.format(COLLECTOR_FORMAT, collectorKey), JsonObject.class);
      String collectorClazz = collectorJson.getString(COLLECTOR_CLAZZ);
      JsonObject collectorNet = jsonGetValue(collectorJson, COLLECTOR_CONFIG, JsonObject.class);
      List<String> klineSubscribe = jsonListValue(collectorJson, COLLECTOR_SUBSCRIBE_KLINE, String.class);
      List<String> depthSubscribe = jsonListValue(collectorJson, COLLECTOR_SUBSCRIBE_DEPTH, String.class);
      List<String> tradeDetailSubscribe = jsonListValue(collectorJson, COLLECTOR_SUBSCRIBE_TRADE_DETAIL, String.class);
      if (collectorClazz != null && !collectorClazz.isEmpty()) {
        log.info("[Collectors]: deploy collector: " + collectorClazz);
        Future<Void> deployFuture = openService.deployCollectorEx(collectorClazz, collectorNet);
        deployFuture.compose(none -> {
          List<Future> subscribeFutures = new ArrayList<>();
          for (String subscribeSymbol : klineSubscribe) {
            subscribeFutures.add(openService.subscribe(collectorClazz, DataType.KLINE, subscribeSymbol));
          }
          for (String subscribeSymbol : depthSubscribe) {
            subscribeFutures.add(openService.subscribe(collectorClazz, DataType.DEPTH, subscribeSymbol));
          }
          for (String subscribeSymbol : tradeDetailSubscribe) {
            subscribeFutures.add(openService.subscribe(collectorClazz, DataType.TRADE_DETAIL, subscribeSymbol));
          }
          return CompositeFuture.any(subscribeFutures);
        });
        collectorsFutures.add(deployFuture);
      }
    }

    CompositeFuture.any(collectorsFutures)
        .compose(none -> vertx.deployVerticle(new CollectorOpenApiHttpEndpoint(openService),
            new DeploymentOptions().setConfig(jsonGetValue(config(), OPEN_API_HTTP_ENDPOINT_CONFIG, JsonObject.class)))
        )
        .onSuccess(ignored -> {
          log.info("[Collectors]: all collector deploy success!");
          promise.complete();
        })
        .onFailure(Throwable::printStackTrace);
  }

  public void handlerHello(RoutingContext ctx) {
    ///

  }
}
