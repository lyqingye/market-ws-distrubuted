package com.tqxd.jys.messagebus.impl.kafka;


import com.tqxd.jys.messagebus.MessageBus;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.topic.Topic;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.shareddata.Counter;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * kafka 事件总线实现
 */
public class KafkaMessageBusImpl implements MessageBus {
  public static final String MESSAGE_INDEX_COUNTER_NAME = "kafka_message_index_counter";
  private static final Logger log = LoggerFactory.getLogger(KafkaMessageBusImpl.class);
  private Map<String, KafkaConsumer<String, Object>> consumerMap = new ConcurrentHashMap<>();
  private KafkaProducer<String, Object> producer;
  private Vertx vertx;
  private Counter messageIndexCounter;
  private Map<String, String> consumerConfig, producerConfig;

  public KafkaMessageBusImpl(Vertx vertx, Map<String, String> consumerConfig, Map<String, String> producerConfig) {
    this.vertx = Objects.requireNonNull(vertx);
    this.consumerConfig = Objects.requireNonNull(consumerConfig);
    this.producerConfig = Objects.requireNonNull(producerConfig);
    producer = KafkaProducer.create(vertx, producerConfig);
    vertx.sharedData()
        .getCounter(MESSAGE_INDEX_COUNTER_NAME)
        .onSuccess(h -> messageIndexCounter = h)
        .onFailure(Throwable::printStackTrace);
  }

  @Override
  public void publish(Topic topic, Message<?> message, Handler<AsyncResult<Void>> handler) {
    while (messageIndexCounter == null) {
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
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
  public void unSubscribe(Topic topic, String registryId, Handler<AsyncResult<Void>> handler) {
    KafkaConsumer<String, Object> consumer = consumerMap.get(registryId);
    if (consumer == null) {
      handler.handle(Future.failedFuture("consumer not found"));
    } else {
      consumer.unsubscribe(handler);
    }
  }
}
