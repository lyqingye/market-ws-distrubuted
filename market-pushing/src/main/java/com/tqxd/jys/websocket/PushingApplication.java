package com.tqxd.jys.websocket;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PushingApplication extends AbstractVerticle {


  private static final Logger log = LoggerFactory.getLogger(PushingApplication.class);

  public static void main(String[] args) {
    Map<String, String> kafkaConfig = new HashMap<>();
    kafkaConfig.put("bootstrap.servers", "b-1.pre-new-jys-match-up-1.cdcxjy.c2.kafka.ap-east-1.amazonaws.com:9092");
    kafkaConfig.put("key.serializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonSerializer");
    kafkaConfig.put("value.serializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonSerializer");
    kafkaConfig.put("key.deserializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonDeSerializer");
    kafkaConfig.put("value.deserializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonDeSerializer");
    kafkaConfig.put("group.id", "test2021-4-27");
    kafkaConfig.put("auto.offset.reset", "earliest");
    kafkaConfig.put("enable.auto.commit", "true");
    Vertx vertx = Vertx.vertx();
    KafkaConsumer<String, String> kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig);
    kafkaConsumer.handler(msg -> {
      System.out.println(msg.value());
    })
        .exceptionHandler(Throwable::printStackTrace);
    kafkaConsumer.subscribe("balances")
        .onSuccess(h -> {

        })
        .onFailure(Throwable::printStackTrace);


//    JsonObject zkConfig = new JsonObject();
//    zkConfig.put("zookeeperHosts", "127.0.0.1");
//    zkConfig.put("rootPath", "io.vertx");
//    zkConfig.put("retry", new JsonObject()
//        .put("initialSleepTime", 3000)
//        .put("maxTimes", 3));
//    ClusterManager mgr = new ZookeeperClusterManager(zkConfig);
//    VertxOptions options = new VertxOptions().setClusterManager(mgr);
//    options.setClusterManager(mgr);
//    Vertx.clusteredVertx(options, ar -> {
//      if (ar.succeeded()) {
//        Vertx vertx = ar.result();
//        try {
//          VertxUtil.readYamlConfig(vertx, "config.yaml", h -> {
//            if (h.succeeded()) {
//              MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx, kafkaConfig, kafkaConfig);
//              VertxUtil.deploy(vertx, new PushingApplication(), h.result())
//                  .onFailure(Throwable::printStackTrace);
//
//            } else {
//              h.cause().printStackTrace();
//            }
//          });
//        } catch (ExecutionException | InterruptedException e) {
//          e.printStackTrace();
//        }
//      } else {
//        ar.cause().printStackTrace();
//      }
//    });
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertx.deployVerticle(new KlineWorkerVerticle(),new DeploymentOptions().setWorker(true));
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
  }
}
