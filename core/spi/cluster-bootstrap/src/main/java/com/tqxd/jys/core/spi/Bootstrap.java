package com.tqxd.jys.core.spi;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;

/**
 * 启动器
 *
 * @author lyqingye
 */
public interface Bootstrap {
  void start(Verticle verticle, DeploymentOptions options);
}
