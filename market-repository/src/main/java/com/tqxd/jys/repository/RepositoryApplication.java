package com.tqxd.jys.repository;

import com.tqxd.jys.bootstrap.Bootstrap;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.repository.faced.EventBusRepositoryFaced;
import com.tqxd.jys.repository.impl.CacheableKLineRepositoryProxy;
import com.tqxd.jys.repository.impl.RedisKLineRepository;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.sync.MBKLineRepositoryAppendedSyncer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lyqingye
 */
@SuppressWarnings("unchecked")
public class RepositoryApplication extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(RepositoryApplication.class);
  private KLineRepository kLineRepository;
  private EventBusRepositoryFaced ebRepositoryFaced;
  private String msgBusRegistryId;

  public static void main(String[] args) {
    Bootstrap.run(new RepositoryApplication(), new DeploymentOptions().setWorker(true));
  }


  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    kLineRepository = new CacheableKLineRepositoryProxy(new RedisKLineRepository());
    ebRepositoryFaced = new EventBusRepositoryFaced(kLineRepository);
    MBKLineRepositoryAppendedSyncer syncer = new MBKLineRepositoryAppendedSyncer();
    kLineRepository
      .open(vertx, config())
      // 将k线数据注册到k线仓库同步器
      .compose(none -> MessageBusFactory.bus().subscribe(Topic.KLINE_TICK_TOPIC, syncer))
      // 注册 eventbus open api
      .compose(registryId -> {
        log.info("[RepositoryApplication]: register message bus success! registryId: {}", msgBusRegistryId);
        ebRepositoryFaced.register(vertx);
        log.info("[RepositoryApplication]: register eventbus faced success!");
        // 同步数据到仓库
        return syncer.syncAppendTo(kLineRepository);
      })
      .onComplete(startPromise);
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    ebRepositoryFaced.unregister();
    MessageBusFactory.bus()
      .unSubscribe(Topic.KLINE_TICK_TOPIC,msgBusRegistryId, ar -> {
        if (ar.succeeded()) {
          kLineRepository.close(stopPromise);
        }else {
          stopPromise.fail(ar.cause());
        }
      });
  }
}
