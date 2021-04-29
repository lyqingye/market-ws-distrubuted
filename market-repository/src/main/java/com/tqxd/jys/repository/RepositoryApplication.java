package com.tqxd.jys.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.common.payload.TemplatePayload;
import com.tqxd.jys.messagebus.MessageBusFactory;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.topic.Topic;
import com.tqxd.jys.repository.openapi.RepositoryOpenApiImpl;
import com.tqxd.jys.repository.redis.RedisHelper;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.JacksonCodec;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author lyqingye
 */
public class RepositoryApplication extends AbstractVerticle {
  private static final Logger log = LoggerFactory.getLogger(RepositoryApplication.class);


  public static void main(String[] args) {
    Map<String, String> kafkaConfig = new HashMap<>();
    kafkaConfig.put("bootstrap.servers", "localhost:9092");
    kafkaConfig.put("key.serializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonSerializer");
    kafkaConfig.put("value.serializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonSerializer");
    kafkaConfig.put("key.deserializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonDeSerializer");
    kafkaConfig.put("value.deserializer", "com.tqxd.jys.messagebus.impl.kafka.KafkaJsonDeSerializer");
    kafkaConfig.put("group.id", "repository");
    kafkaConfig.put("auto.offset.reset", "earliest");
    kafkaConfig.put("enable.auto.commit", "true");

    JsonObject zkConfig = new JsonObject();
    zkConfig.put("zookeeperHosts", "127.0.0.1");
    zkConfig.put("rootPath", "io.vertx");
    zkConfig.put("retry", new JsonObject()
        .put("initialSleepTime", 3000)
        .put("maxTimes", 3));
    ClusterManager mgr = new ZookeeperClusterManager(zkConfig);
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    options.setClusterManager(mgr);
    Vertx.clusteredVertx(options, ar -> {
      if (ar.succeeded()) {
        Vertx vertx = ar.result();
        try {
          VertxUtil.readYamlConfig(vertx, "config.yaml", h -> {
            if (h.succeeded()) {
              MessageBusFactory.init(MessageBusFactory.KAFKA_MESSAGE_BUS, vertx, kafkaConfig, kafkaConfig);
              VertxUtil.deploy(vertx, new RepositoryApplication(), new DeploymentOptions().setWorker(true).setConfig(h.result()))
                  .onFailure(Throwable::printStackTrace);

            } else {
              h.cause().printStackTrace();
            }
          });
        } catch (ExecutionException | InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        ar.cause().printStackTrace();
      }
    });
  }


  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    JsonObject config = config();
    String redisConnString = "redis://localhost:6379/6";
    if (config != null) {
      redisConnString = VertxUtil.jsonGetValue(config, "market.repository.redis.connectionString", String.class, redisConnString);
    }
    RedisHelper.create(vertx, redisConnString)
        .compose(redis -> KlineRepository.create(vertx, redis))
        .compose(repository -> {
          // 开放Open API
          RepositoryOpenApiImpl.init(vertx, repository);
          // 订阅k线数据
          return MessageBusFactory.bus().subscribe(Topic.KLINE_TICK_TOPIC, data -> processKlineMessage(repository, data));
        })
        .onFailure(startPromise::fail);
  }

  private void processKlineMessage(KlineRepository repository, Message<?> msg) {
    switch (msg.getType()) {
      case KLINE: {
        TemplatePayload<KlineTick> payload = JacksonCodec.decodeValue((String) msg.getPayload(), new TypeReference<TemplatePayload<KlineTick>>() {
        });
        repository.forUpdateKline(msg.getIndex(), msg.getTs(), payload);
        log.info("[Market-Repository]: for update kline msgIndex: {}, payload: {}", msg.getIndex(), msg.getPayload());
        break;
      }
      default:
        log.error("[Market-Repository]: invalid message type from Kline topic! message: {}", msg);
    }
  }
}
