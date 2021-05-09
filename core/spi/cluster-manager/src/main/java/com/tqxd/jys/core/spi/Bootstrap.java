package com.tqxd.jys.core.spi;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 启动器
 *
 * @author lyqingye
 */
public interface Bootstrap {
  Logger log = LoggerFactory.getLogger(Bootstrap.class);

  static void run(Verticle verticle, DeploymentOptions options) {
    ServiceLoader<Bootstrap> bootstraps = ServiceLoader.load(Bootstrap.class);
    Iterator<Bootstrap> it = bootstraps.iterator();
    if (!it.hasNext()) {
      log.warn("could not found bootstrap service! just using default bootstrap");
      Vertx.vertx().deployVerticle(verticle, options);
    } else {
      Bootstrap bootstrap = it.next();
      log.info("using {} bootstrap!", bootstrap.getClass().getSimpleName());
      bootstrap.start(verticle, options);
    }
  }

  void start(Verticle verticle, DeploymentOptions options);
}
