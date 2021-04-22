package com.tqxd.jys.messagebus;

import com.tqxd.jys.messagebus.impl.kafka.KafkaMessageBusImpl;
import io.vertx.core.Vertx;

import java.util.Map;

/**
 * 消息总线工厂
 */
public final class MessageBusFactory {
    public static final String KAFKA_MESSAGE_BUS = "kafka_message_bus";
    /**
     * instance
     */
    private static volatile MessageBus INSTANCE;

    public static MessageBus bus() {
        return INSTANCE;
    }

    /**
     * 初始化消息总线
     *
     * @param implName       实例名 {@link MessageBusFactory#KAFKA_MESSAGE_BUS}
     * @param vertx          vertx
     * @param consumerConfig 消费者配置
     * @param producerConfig 生产者配置
     */
    public static void init(String implName, Vertx vertx, Map<String, String> consumerConfig, Map<String, String> producerConfig) {
        if (KAFKA_MESSAGE_BUS.equals(implName)) {
            INSTANCE = new KafkaMessageBusImpl(vertx, consumerConfig, producerConfig);
        } else {
            throw new UnsupportedOperationException("unSupported " + implName + " implementation");
        }
    }
}
