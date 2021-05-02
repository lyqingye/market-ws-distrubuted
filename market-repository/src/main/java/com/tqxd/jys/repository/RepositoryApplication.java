package com.tqxd.jys.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.repository.impl.CacheableKLineRepositoryProxy;
import com.tqxd.jys.repository.impl.RedisKLineRepository;
import com.tqxd.jys.repository.openapi.RepositoryOpenApiImpl;
import com.tqxd.jys.repository.redis.RedisHelper;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
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

    new CacheableKLineRepositoryProxy(new RedisKLineRepository())
      .open(vertx,config())
      .onSuccess(h -> System.out.println())
      .onFailure(Throwable::printStackTrace);

    JsonObject config = config();
    String redisConnString = "redis://localhost:6379/6";
    if (config != null) {
      redisConnString = VertxUtil.jsonGetValue(config, "market.repository.redis.connectionString", String.class, redisConnString);
    }
    RedisHelper.create(vertx, redisConnString)
        .compose(redis -> KlineRepositoryImpl.create(vertx, redis))
        .compose(repository -> {
          // 开放Open API
          RepositoryOpenApiImpl.init(vertx, repository);
          // 订阅k线数据
          return MessageBusFactory.bus().subscribe(Topic.KLINE_TICK_TOPIC, data -> processKlineMessage(repository, data));
        })
        .onFailure(startPromise::fail);
  }

  private void processKlineMessage(KlineRepositoryImpl repository, Message<?> msg) {
    switch (msg.getType()) {
      case KLINE: {
        TemplatePayload<KlineTick> payload = JacksonCodec.decodeValue((String) msg.getPayload(), new TypeReference<TemplatePayload<KlineTick>>() {
        });
        repository.forUpdateKline(msg.getIndex(), msg.getTs(), payload);
//        log.info("[Market-Repository]: for update kline msgIndex: {}, payload: {}", msg.getIndex(), msg.getPayload());
        break;
      }
      default:
        log.error("[Market-Repository]: invalid message type from Kline topic! message: {}", msg);
    }
  }
}
