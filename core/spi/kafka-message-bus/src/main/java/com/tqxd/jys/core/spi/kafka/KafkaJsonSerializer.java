package com.tqxd.jys.core.spi.kafka;

import io.vertx.core.json.Json;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaJsonSerializer implements Serializer<Object> {

  @Override
  public byte[] serialize(String topic, Object data) {
    return Json.encode(data).getBytes();
  }
}
