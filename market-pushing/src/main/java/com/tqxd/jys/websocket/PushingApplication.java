package com.tqxd.jys.websocket;

import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.timeline.KLineManager;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.tqxd.jys.utils.VertxUtil.*;
@SuppressWarnings("unchecked")
public class PushingApplication extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(PushingApplication.class);

  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(new ZookeeperClusterManager("zookeeper.json")),
      clusteredAr -> {
        if (clusteredAr.succeeded()) {
          Vertx vertx = clusteredAr.result();
          // 读取kafka配置
          readJsonFile(vertx, "kafka-consumer.json")
            .compose(
              consumerJson -> readJsonFile(vertx, "kafka-producer.json")
                .compose(producerJson -> {
                  // 初始化消息队列
                  MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx, consumerJson.mapTo(Map.class), producerJson.mapTo(Map.class));
                  Exception ex;
                  try {
                    // 读取yaml配置，然后部署 verticle
                    return readYamlConfig(vertx, "config.yaml")
                      .compose(yamlConfig -> deploy(vertx, new PushingApplication(), yamlConfig));
                  } catch (ExecutionException | InterruptedException e) {
                    ex = e;
                  }
                  return Future.failedFuture(ex);
                })
            )
            .onSuccess(id -> log.info("[PushingApplication]: start success! using: {}ms", System.currentTimeMillis() - start))
            .onFailure(Throwable::printStackTrace);
        } else {
          clusteredAr.cause().printStackTrace();
          System.exit(-1);
        }
      });
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    KLineManager klineManager;
    // 初始化k线管理器
    klineManager = KLineManager.create();
    vertx.deployVerticle(new KLineWorkerVerticle(klineManager), new DeploymentOptions().setWorker(true))
        .compose(c -> vertx.deployVerticle(new ServerEndpointVerticle(klineManager), new DeploymentOptions().setWorker(true))
    ).onFailure(throwable -> {
      log.error("[PushingApplication]: start fail! system will be exit! cause by: {}",throwable.getMessage());
      throwable.printStackTrace();
      System.exit(-1);
    });
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
  }
}
