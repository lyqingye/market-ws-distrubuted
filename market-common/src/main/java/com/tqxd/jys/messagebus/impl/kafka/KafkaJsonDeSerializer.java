package com.tqxd.jys.messagebus.impl.kafka;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaJsonDeSerializer implements Deserializer<Object> {
    @Override
    public Object deserialize(java.lang.String topic, byte[] data) {
        return Json.decodeValue(Buffer.buffer(data));
    }
}
