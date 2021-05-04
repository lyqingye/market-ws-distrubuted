package com.tqxd.jys.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.MessageListener;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.repository.faced.EventBusRepositoryFaced;
import com.tqxd.jys.repository.impl.CacheableKLineRepositoryProxy;
import com.tqxd.jys.timeline.KLineRepository;
import com.tqxd.jys.repository.impl.RedisKLineRepository;
import com.tqxd.jys.utils.ChannelUtil;
import io.vertx.core.*;
import io.vertx.core.json.jackson.JacksonCodec;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.tqxd.jys.utils.VertxUtil.*;

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
                      .compose(yamlConfig -> deploy(vertx, new RepositoryApplication(), yamlConfig));
                  } catch (ExecutionException | InterruptedException e) {
                    ex = e;
                  }
                  return Future.failedFuture(ex);
                })
            )
            .onSuccess(id -> log.info("[RepositoryApplication]: start success! using: {}ms", System.currentTimeMillis() - start))
            .onFailure(Throwable::printStackTrace);
        } else {
          clusteredAr.cause().printStackTrace();
          System.exit(-1);
        }
      });
  }


  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    kLineRepository = new CacheableKLineRepositoryProxy(new RedisKLineRepository());
    ebRepositoryFaced = new EventBusRepositoryFaced(kLineRepository);
    kLineRepository
      .open(vertx,config())
      .compose(h -> MessageBusFactory.bus()
        .subscribe(Topic.KLINE_TICK_TOPIC,(MessageListener) msg -> {
          switch (msg.getType()) {
            case KLINE: {
              TemplatePayload<KlineTick> payload = JacksonCodec.decodeValue((String) msg.getPayload(), new TypeReference<TemplatePayload<KlineTick>>() {
              });
              kLineRepository.append(msg.getIndex(), ChannelUtil.getSymbol(payload.getCh()), Period._1_MIN, payload.getTick())
                .onFailure(Throwable::printStackTrace);
              break;
            }
            default:
              log.error("[RepositoryApplication]: invalid message type from Kline topic! message: {}", msg);
          }
        }))
      // 注册 eventbus open api
      .onSuccess(registryId -> {
        msgBusRegistryId = registryId;
        log.info("[RepositoryApplication]: register message bus success! registryId: {}", msgBusRegistryId);
        ebRepositoryFaced.register(vertx);
        log.info("[RepositoryApplication]: register eventbus faced success!");
        startPromise.complete();
      })
      .onFailure(startPromise::fail);
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
