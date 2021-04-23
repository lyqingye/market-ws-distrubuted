package com.tqxd.jys.websocket;

import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.openapi.RepositoryOpenApi;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.Json;
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
    RepositoryOpenApi openAPI = RepositoryOpenApi.createProxy(vertx);
    openAPI.listKlineKeys(ar -> {
      if (ar.succeeded()) {
        System.out.println(ar.result());
        for (String klineKey : ar.result()) {
          openAPI.getKlineSnapshot(klineKey, ar2 -> {
            if (ar2.succeeded()) {
              KlineSnapshot snapshot = Json.decodeValue(ar2.result(), KlineSnapshot.class);
              System.out.println(ar2.result());
            } else {
              ar2.cause().printStackTrace();
            }
          });
        }
      } else {
        ar.cause().printStackTrace();
      }
    });
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
  }
}
