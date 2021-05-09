package com.tqxd.jys.core.spi.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.tqxd.jys.core.spi.Bootstrap;
import com.tqxd.jys.tqxd.core.utils.VertxUtil;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tqxd.jys.tqxd.core.utils.VertxUtil.deploy;

public class HazelcastBootstrap implements Bootstrap {
  private static final Logger log = LoggerFactory.getLogger(HazelcastBootstrap.class);

  @Override
  public void start(Verticle verticle, DeploymentOptions options) {
    long start = System.currentTimeMillis();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    HazelcastClusterManager hazelcastClusterManager = new HazelcastClusterManager(hazelcastInstance);
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(hazelcastClusterManager),
      clusteredAr -> {
        if (clusteredAr.succeeded()) {
          Vertx vertx = clusteredAr.result();
          VertxUtil.readYamlConfig(vertx, "config.yaml")
            .compose(yamlConfig -> deploy(vertx, verticle, options.setConfig(yamlConfig)))
            .onFailure(throwable -> {
              throwable.printStackTrace();
              System.exit(-1);
            })
            .onSuccess(id -> log.info("[HazelcastBootstrap]: start success! using: {}ms", System.currentTimeMillis() - start));
        } else {
          clusteredAr.cause().printStackTrace();
          System.exit(-1);
        }
      });
  }
}
