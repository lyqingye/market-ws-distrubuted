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

public class PushingApplication extends AbstractVerticle {


  private static final Logger log = LoggerFactory.getLogger(PushingApplication.class);

  public static void main(String[] args) {
    Map<String, String> kafkaConfig = new HashMap<>();
    kafkaConfig.put("bootstrap.servers", "localhost:9092");
    kafkaConfig.put("key.serializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonSerializer");
    kafkaConfig.put("value.serializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonSerializer");
    kafkaConfig.put("key.deserializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonDeSerializer");
    kafkaConfig.put("value.deserializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonDeSerializer");
    kafkaConfig.put("group.id", "pushing");
    kafkaConfig.put("auto.offset.reset", "earliest");
    kafkaConfig.put("enable.auto.commit", "true");

    JsonObject zkConfig = new JsonObject();
    zkConfig.put("zookeeperHosts", "127.0.0.1");
    zkConfig.put("rootPath", "io.vertx");
    zkConfig.put("retry", new JsonObject()
        .put("initialSleepTime", 3000)
        .put("maxTimes", 3));
    ClusterManager mgr = new ZookeeperClusterManager(zkConfig);
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    options.setClusterManager(mgr);
    options.setEventBusOptions(new EventBusOptions().setConnectTimeout(5000));
    Vertx.clusteredVertx(options, ar -> {
      if (ar.succeeded()) {
        Vertx vertx = ar.result();
        try {
          VertxUtil.readYamlConfig(vertx, "config.yaml", h -> {
            if (h.succeeded()) {
              MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx, kafkaConfig, kafkaConfig);
              VertxUtil.deploy(vertx, new PushingApplication(), h.result())
                  .onFailure(Throwable::printStackTrace);

            } else {
              h.cause().printStackTrace();
            }
          });
        } catch (ExecutionException | InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        ar.cause().printStackTrace();
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
