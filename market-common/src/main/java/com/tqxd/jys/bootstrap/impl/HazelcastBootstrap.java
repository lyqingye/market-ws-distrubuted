package com.tqxd.jys.bootstrap.impl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.tqxd.jys.bootstrap.Bootstrap;
import com.tqxd.jys.messagebus.MessageBusFactory;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.tqxd.jys.utils.VertxUtil.*;

@SuppressWarnings("unchecked")
public class HazelcastBootstrap implements Bootstrap {
  private static final Logger log = LoggerFactory.getLogger(HazelcastBootstrap.class);
  private static final String VERTX_CLUSTER_PUBLIC_HOST = "vertx.cluster.public.host";

  @Override
  public void start(Verticle verticle, DeploymentOptions options) {
    long start = System.currentTimeMillis();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    HazelcastClusterManager hazelcastClusterManager = new HazelcastClusterManager(hazelcastInstance);
    VertxOptions vertxOptions = new VertxOptions().setClusterManager(hazelcastClusterManager);
    String host = System.getenv(VERTX_CLUSTER_PUBLIC_HOST);
    if (host == null) {
      host = System.getProperty(VERTX_CLUSTER_PUBLIC_HOST);
    }
    EventBusOptions eventBusOptions = new EventBusOptions().setHost(host).setClusterPublicHost(host);
    vertxOptions.setEventBusOptions(eventBusOptions);
    String finalHost = host;
    Vertx.clusteredVertx(vertxOptions,
        clusteredAr -> {
          if (clusteredAr.succeeded()) {
            Vertx vertx = clusteredAr.result();
            readJsonFiles(vertx, new String[]{"kafka-consumer.json", "kafka-producer.json"})
                .compose(objects -> MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx,
                    objects[0].mapTo(Map.class), objects[1].mapTo(Map.class)))
                .compose(none -> readYamlConfig(vertx, "config.yaml")
                    .compose(yamlConfig -> deploy(vertx, verticle, options.setConfig(yamlConfig)))
                    .onFailure(throwable -> {
                      throwable.printStackTrace();
                      System.exit(-1);
                    }))
                .onSuccess(id -> log.info("[HazelcastBootstrap]: start success! cluster host: {} using: {}ms", finalHost, System.currentTimeMillis() - start));
          } else {
            clusteredAr.cause().printStackTrace();
            System.exit(-1);
          }
        });
  }
}
