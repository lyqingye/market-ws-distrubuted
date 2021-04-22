package com.tqxd.jys.messagebus.impl.kafka;

import org.apache.kafka.common.serialization.Deserializer;

public class KafkaJsonDeSerializer implements Deserializer<Object> {
    @Override
    public Object deserialize(java.lang.String topic, byte[] data) {
        return new String(data);
    }
}
