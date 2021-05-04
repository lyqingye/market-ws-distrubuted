package com.tqxd.jys.collectors;

import com.tqxd.jys.bootstrap.Bootstrap;
import com.tqxd.jys.collectors.openapi.CollectorOpenApiImpl;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.openapi.CollectorOpenApi;
import com.tqxd.jys.openapi.ServiceAddress;
import com.tqxd.jys.openapi.payload.CollectorStatusDto;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.tqxd.jys.utils.VertxUtil.jsonGetValue;
import static com.tqxd.jys.utils.VertxUtil.jsonListValue;

/**
 * @author yjt
 * @since 2020/10/10 下午3:45
 */
@SuppressWarnings("unchecked")
public class CollectorsApplication extends AbstractVerticle {
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
    String collectorName = jsonGetValue(config, "market.collector.name", String.class);
    List<String> klineSubscribe = jsonListValue(config, "market.collector.subscribe.kline", String.class);
    List<String> depthSubscribe = jsonListValue(config, "market.collector.subscribe.depth", String.class);
    List<String> tradeDetailSubscribe = jsonListValue(config, "market.collector.subscribe.trade.detail", String.class);
    if (collectorName != null && !collectorName.isEmpty()) {
      log.info("[Collectors]: deploy collector: " + collectorName);
      Future<Boolean> future = openService.deployCollector(collectorName).compose(ignored -> openService.startCollector(collectorName));
      for (String subscribeSymbol : klineSubscribe) {
        future = future.compose(none -> openService.subscribe(collectorName, DataType.KLINE, subscribeSymbol));
      }
      for (String subscribeSymbol : depthSubscribe) {
        future = future.compose(none -> openService.subscribe(collectorName, DataType.DEPTH, subscribeSymbol));
      }
      for (String subscribeSymbol : tradeDetailSubscribe) {
        future = future.compose(none -> openService.subscribe(collectorName, DataType.TRADE_DETAIL, subscribeSymbol));
      }
      future.onFailure(promise::fail);
      future.onSuccess(ignored -> {
        log.info("[Collectors]: all subscribe success!");
        promise.complete();
      });
    }
  }
}
