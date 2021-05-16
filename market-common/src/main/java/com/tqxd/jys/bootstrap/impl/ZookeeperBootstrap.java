package com.tqxd.jys.bootstrap.impl;

import com.tqxd.jys.bootstrap.Bootstrap;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zookeeper boostrap 启动器
 *
 * @author lyqingye
 */
@SuppressWarnings("unchecked")
public class ZookeeperBootstrap implements Bootstrap {
  private static final Logger log = LoggerFactory.getLogger(ZookeeperBootstrap.class);

  @Override
  public void start(Verticle verticle, DeploymentOptions options) {
//    long start = System.currentTimeMillis();
//    Vertx.clusteredVertx(new VertxOptions().setClusterManager(new ZookeeperClusterManager("zookeeper.json")),
//      clusteredAr -> {
//        if (clusteredAr.succeeded()) {
//          Vertx vertx = clusteredAr.result();
//          readJsonFiles(vertx, new String[]{"kafka-consumer.json", "kafka-producer.json"})
//            .compose(objects -> MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx,
//              objects[0].mapTo(Map.class), objects[1].mapTo(Map.class)))
//            .compose(none -> readYamlConfig(vertx, "config.yaml")
//              .compose(yamlConfig -> deploy(vertx, verticle, options.setConfig(yamlConfig)))
//              .onFailure(throwable -> {
//                throwable.printStackTrace();
//                System.exit(-1);
//              }))
//            .onSuccess(id -> log.info("[ZookeeperBootstrap]: start success! using: {}ms", System.currentTimeMillis() - start));
//        } else {
//          clusteredAr.cause().printStackTrace();
//          System.exit(-1);
//        }
//      });
  }
}
