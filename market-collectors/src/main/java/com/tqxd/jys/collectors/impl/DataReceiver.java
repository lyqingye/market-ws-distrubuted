package com.tqxd.jys.collectors.impl;

import com.tqxd.jys.constance.DataType;
import io.vertx.core.json.JsonObject;

public interface DataReceiver {

  default void onReceive(Collector from, DataType dataType, JsonObject obj) {
  }
}
