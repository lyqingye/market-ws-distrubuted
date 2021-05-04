package com.tqxd.jys.bootstrap;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * 启动器
 *
 * @author lyqingye
 */
public interface Bootstrap {
  Logger log = LoggerFactory.getLogger(Bootstrap.class);

  @SuppressWarnings("unchecked")
  static void run(Verticle verticle, DeploymentOptions options) {
    try {
      Class<Bootstrap> zookeeper = (Class<Bootstrap>) Class.forName("com.tqxd.jys.bootstrap.impl.ZookeeperBootstrap");
      for (Constructor<?> constructor : zookeeper.getConstructors()) {
        if (constructor.getParameterCount() == 0) {
          Bootstrap bootstrap = (Bootstrap) constructor.newInstance();
          log.info("bootstrap using {}", "com.tqxd.jys.ZookeeperBootstrap");
          bootstrap.start(verticle, options);
          return;
        }
      }
    } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException ignored) {

    }
    log.error("bootstrap fail! not provider! system will be exit!");
    System.exit(-1);
  }

  void start(Verticle verticle, DeploymentOptions options);
}
