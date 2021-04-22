package com.tqxd.jys.messagebus.impl.kafka;

import io.vertx.core.json.Json;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaJsonSerializer implements Serializer<Object> {

  @Override
  public byte[] serialize(java.lang.String topic, Object data) {
    return Json.encode(data).getBytes();
  }
}
