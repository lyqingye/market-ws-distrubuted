package com.tqxd.jys.websocket;

import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.timeline.InMemKLineRepository;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.KLineRepositoryAdapter;
import com.tqxd.jys.timeline.sync.MBKLineRepositoryAppendedSyncer;
import com.tqxd.jys.websocket.cache.CacheManager;
import com.tqxd.jys.websocket.transport.ServerEndpointVerticle;
import io.vertx.core.*;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.tqxd.jys.utils.VertxUtil.*;
@SuppressWarnings("unchecked")
public class PushingApplication extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(PushingApplication.class);
  private KLineRepository kLineRepository;
  private CacheManager cacheManager;
  private ServerEndpointVerticle serverEndpointVerticle;

  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(new ZookeeperClusterManager("zookeeper.json")),
      clusteredAr -> {
        if (clusteredAr.succeeded()) {
          Vertx vertx = clusteredAr.result();
          // 读取kafka配置
          readJsonFile(vertx, "kafka-consumer.json")
            .compose(
              consumerJson -> readJsonFile(vertx, "kafka-producer.json")
                .compose(producerJson -> {
                  // 初始化消息队列
                  MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx, consumerJson.mapTo(Map.class), producerJson.mapTo(Map.class));
                  Exception ex;
                  try {
                    // 读取yaml配置，然后部署 verticle
                    return readYamlConfig(vertx, "config.yaml")
                      .compose(yamlConfig -> deploy(vertx, new PushingApplication(), yamlConfig));
                  } catch (ExecutionException | InterruptedException e) {
                    ex = e;
                  }
                  return Future.failedFuture(ex);
                })
            )
            .onSuccess(id -> log.info("[PushingApplication]: start success! using: {}ms", System.currentTimeMillis() - start))
            .onFailure(Throwable::printStackTrace);
        } else {
          clusteredAr.cause().printStackTrace();
          System.exit(-1);
        }
      });
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    long start = System.currentTimeMillis();
    CompositeFuture.join(initKLineRepository(),initCacheManager(),initTransport())
      .onSuccess(none -> {
        log.info("[PushingApplication]: start success! using {}ms",System.currentTimeMillis() - start);
      })
      .onFailure(throwable -> {
        log.error("[PushingApplication]: start fail! system will be exit! cause by: {}", throwable.getMessage());
        throwable.printStackTrace();
        System.exit(-1);
      }
    );
  }

  private Future<Void> initKLineRepository () {
    KLineRepositoryAdapter remote = new KLineRepositoryAdapter();
    kLineRepository = new InMemKLineRepository();
    MBKLineRepositoryAppendedSyncer syncer = new MBKLineRepositoryAppendedSyncer();
    return remote.open(vertx, config())
      .compose(none -> kLineRepository.open(vertx, config()))
      .compose(none -> kLineRepository.importFrom(remote))
      // 将k线数据注册到k线仓库同步器
      .compose(none -> MessageBusFactory.bus().subscribe(Topic.KLINE_TICK_TOPIC, syncer))
      // 同步数据到k线仓库
      .compose(none -> syncer.syncAppendTo(kLineRepository));
  }

  private Future<Void> initCacheManager () {
    cacheManager = new CacheManager(kLineRepository);
    return MessageBusFactory.bus().subscribe(Topic.TRADE_DETAIL_TOPIC, cacheManager)
      .compose(none -> MessageBusFactory.bus().subscribe(Topic.DEPTH_CHART_TOPIC, cacheManager))
      .map(toVoid -> null);
  }

  private Future<Void> initTransport () {
    this.serverEndpointVerticle = new ServerEndpointVerticle(cacheManager);
    return deploy(vertx,serverEndpointVerticle , new DeploymentOptions().setWorker(true))
      .map(toVoid -> null);
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
  }
}
