package com.tqxd.jys.messagebus.impl;


import com.tqxd.jys.messagebus.MessageBus;
import com.tqxd.jys.messagebus.payload.Message;
import com.tqxd.jys.messagebus.topic.Topic;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * kafka 事件总线实现
 */
public class KafkaMessageBusImpl implements MessageBus {

    private Map<String, KafkaConsumer<String, Object>> consumerMap = new ConcurrentHashMap<>();
    private KafkaProducer<String, Object> producer;
    private Vertx vertx;
    private Map<String,String> kafkaConfig;

    public KafkaMessageBusImpl (Vertx vertx,Map<String, String> kafkaConfig) {
        this.vertx = vertx;
        this.kafkaConfig = kafkaConfig;
        producer = KafkaProducer.create(vertx, kafkaConfig);
    }

    @Override
    public void publish(Topic topic, Message<?> message, Handler<AsyncResult<Void>> handler) {
        KafkaProducerRecord<String, Object> record = KafkaProducerRecord.create(topic.name(), message);
        producer.write(record,handler);
    }

    @Override
    public void subscribe(Topic topic, Consumer<Message<?>> consumer, Handler<AsyncResult<String>> handler) {
        KafkaConsumer<String, Object> c = KafkaConsumer.create(vertx, kafkaConfig);
        String id = UUID.randomUUID().toString();
        c.subscribe(topic.name(),rs -> {
            if (rs.succeeded()) {
                handler.handle(Future.succeededFuture(id));
                consumerMap.put(id,c);
            }else {
                handler.handle(Future.failedFuture(rs.cause()));
            }
        }).handler(record -> {
            if (consumer != null) {
                record.value();
            }
        }).exceptionHandler(Throwable::printStackTrace);
    }

    @Override
    public void unSubscribe(Topic topic, String registryId, Handler<AsyncResult<Void>> handler) {
        KafkaConsumer<String, Object> consumer = consumerMap.get(registryId);
        if (consumer == null) {
            handler.handle(Future.failedFuture("consumer not found"));
        }else {
            consumer.unsubscribe(handler);
        }
    }
}
