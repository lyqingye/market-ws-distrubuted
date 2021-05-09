package com.tqxd.jys.core.spi.kafka;

import com.tqxd.jys.core.spi.Message;
import com.tqxd.jys.core.spi.MessageBus;
import com.tqxd.jys.core.spi.MessageListener;
import com.tqxd.jys.core.spi.Topic;
import com.tqxd.jys.tqxd.core.utils.VertxUtil;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.Counter;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * kafka 事件总线实现
 */
public class KafkaMessageBus extends AbstractVerticle implements MessageBus {
  public static final String MESSAGE_INDEX_COUNTER_NAME = "kafka_message_index_counter";
  private static final Logger log = LoggerFactory.getLogger(KafkaMessageBus.class);
  private Map<String, KafkaConsumer<String, Object>> consumerMap = new ConcurrentHashMap<>();
  private KafkaProducer<String, Object> producer;
  private Counter messageIndexCounter;
  private Map<String, String> consumerConfig, producerConfig;

  @Override
  @SuppressWarnings("unchecked")
  public void start(Promise<Void> startPromise) throws Exception {
    VertxUtil.readJsonFiles(vertx, new String[]{"kafka-consumer.json", "kafka-producer.json"})
      .compose(objects -> {
        try {
          consumerConfig = objects[0].mapTo(Map.class);
          producerConfig = objects[1].mapTo(Map.class);
          producer = KafkaProducer.create(vertx, producerConfig);
        } catch (Exception ex) {
          return Future.failedFuture(ex);
        }
        return Future.succeededFuture();
      })
      .compose(none -> vertx.sharedData().getCounter(MESSAGE_INDEX_COUNTER_NAME))
      .onSuccess(counter -> {
        messageIndexCounter = counter;
        startPromise.complete();
      })
      .onFailure(startPromise::fail);
  }

  @Override
  public void publish(Topic topic, Message<?> message, Handler<AsyncResult<Void>> handler) {
    messageIndexCounter.addAndGet(1)
      .compose(idx -> {
        message.setIndex(idx);
        KafkaProducerRecord<String, Object> record = KafkaProducerRecord.create(topic.name(), message);
        return producer.write(record);
      })
      .onSuccess(h -> {
        handler.handle(Future.succeededFuture());
      })
      .onFailure(throwable -> {
        handler.handle(Future.failedFuture(throwable));
      });
  }

  @Override
  public void subscribe(Topic topic, Consumer<Message<?>> consumer, Handler<AsyncResult<String>> handler) {
    KafkaConsumer<String, Object> c = KafkaConsumer.create(vertx, consumerConfig);
    String id = UUID.randomUUID().toString();
    c.subscribe(topic.name(), rs -> {
      if (rs.succeeded()) {
        handler.handle(Future.succeededFuture(id));
        consumerMap.put(id, c);
      } else {
        handler.handle(Future.failedFuture(rs.cause()));
      }
    }).handler(record -> {
      if (consumer != null) {
        Object value = record.value();
        if (value instanceof String) {
          try {
            consumer.accept(Json.decodeValue((String) value, Message.class));
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        } else {
          log.error("[KafkaMessageBus]: value must be instance of java.lang.String! msg: {}", value);
        }
      }
    }).exceptionHandler(Throwable::printStackTrace);
  }

  @Override
  public void subscribe(Topic topic, MessageListener listener, Handler<AsyncResult<String>> handler) {
    subscribe(topic, (Consumer<Message<?>>) listener::onMessage, handler);
  }

  @Override
  public void unSubscribe(Topic topic, String registryId, Handler<AsyncResult<Void>> handler) {
    KafkaConsumer<String, Object> consumer = consumerMap.get(registryId);
    if (consumer == null) {
      handler.handle(Future.failedFuture("consumer not found"));
    } else {
      consumer.unsubscribe(handler);
    }
  }
}
