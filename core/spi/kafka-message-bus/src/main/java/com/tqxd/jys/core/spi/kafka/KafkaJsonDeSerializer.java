package com.tqxd.jys.core.spi.kafka;

import org.apache.kafka.common.serialization.Deserializer;

public class KafkaJsonDeSerializer implements Deserializer<Object> {
  @Override
  public Object deserialize(String topic, byte[] data) {
    return new String(data);
  }
}
