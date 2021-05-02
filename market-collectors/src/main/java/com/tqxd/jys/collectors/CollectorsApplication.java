package com.tqxd.jys.collectors;

import com.tqxd.jys.collectors.openapi.CollectorOpenApiImpl;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.openapi.CollectorOpenApi;
import com.tqxd.jys.openapi.ServiceAddress;
import com.tqxd.jys.openapi.payload.CollectorStatusDto;
import com.tqxd.jys.utils.TimeUtils;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
    long start = System.currentTimeMillis();
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(new ZookeeperClusterManager("zookeeper.json")),
      clusteredAr -> {
        if (clusteredAr.succeeded()) {
          Vertx vertx = clusteredAr.result();
          // 读取kafka配置
          VertxUtil.readJsonFile(vertx, "kafka-consumer.json")
            .compose(
              consumerJson -> VertxUtil.readJsonFile(vertx, "kafka-producer.json")
                .compose(producerJson -> {
                  // 初始化消息队列
                  MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx, consumerJson.mapTo(Map.class), producerJson.mapTo(Map.class));
                  Exception ex;
                  try {
                    // 读取yaml配置，然后部署 verticle
                    return VertxUtil.readYamlConfig(vertx, "config.yaml")
                      .compose(yamlConfig -> VertxUtil.deploy(vertx, new CollectorsApplication(), yamlConfig));
                  } catch (ExecutionException | InterruptedException e) {
                    ex = e;
                  }
                  return Future.failedFuture(ex);
                })
            )
            .onSuccess(id -> log.info("[CollectorApplication]: start success! using: {}ms", System.currentTimeMillis() - start))
            .onFailure(Throwable::printStackTrace);
        } else {
          clusteredAr.cause().printStackTrace();
          System.exit(-1);
        }
      });
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
    String collectorName = VertxUtil.jsonGetValue(config, "market.collector.name", String.class);
    List<String> subscribe = VertxUtil.jsonListValue(config, "market.collector.subscribe", String.class);
    if (collectorName != null && !collectorName.isEmpty()) {
      Future<Boolean> future = openService.deployCollector(collectorName).compose(ignored -> openService.startCollector(collectorName));
      for (String subscribeSymbol : subscribe) {
        future = future.compose(ignored -> openService.subscribe(collectorName, DataType.KLINE, subscribeSymbol));
      }
      future.onFailure(promise::fail);
      future.onSuccess(ignored -> {
        log.info("[Market-KlineCollector]: start success!");
        log.info("[Market-KlineCollector]: deploy collector: " + collectorName);
        log.info("[Market-KlineCollector]: subscribe: " + subscribe);
        promise.complete();
      });
    }
  }
}
