package com.tqxd.jys.websocket;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.DataType;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.MessageListener;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.timeline.InMemKLineRepository;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.timeline.KLineRepositoryAdapter;
import com.tqxd.jys.utils.ChannelUtil;
import com.tqxd.jys.utils.VertxUtil;
import com.tqxd.jys.websocket.cache.CacheManager;
import com.tqxd.jys.websocket.transport.ServerEndpointVerticle;
import io.vertx.core.*;
import io.vertx.core.json.jackson.JacksonCodec;
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
    return remote.open(vertx, config())
      .compose(none -> kLineRepository.open(vertx, config()))
      .compose(none -> kLineRepository.importFrom(remote))
      .compose(none ->
         MessageBusFactory.bus()
          .subscribe(Topic.KLINE_TICK_TOPIC, (MessageListener) msg -> {
            if (msg.getType() == DataType.KLINE) {
              TemplatePayload<KlineTick> payload = JacksonCodec.decodeValue((String) msg.getPayload(), new TypeReference<TemplatePayload<KlineTick>>() {
              });
              this.kLineRepository.append(msg.getIndex(), ChannelUtil.getSymbol(payload.getCh()), Period._1_MIN, payload.getTick())
                .onFailure(Throwable::printStackTrace);
            } else {
              log.error("[RepositoryApplication]: invalid message type from Kline topic! message: {}", msg);
            }
          })
          .map(toVoid -> null)
      );
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
