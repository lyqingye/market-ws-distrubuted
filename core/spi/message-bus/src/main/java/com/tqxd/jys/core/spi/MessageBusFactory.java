package com.tqxd.jys.core.spi;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * 消息总线工厂
 */
public final class MessageBusFactory {
  private static MessageBus INSTANCE;

  public static Future<MessageBus> create(Vertx vertx) {
    Objects.requireNonNull(vertx);
    Promise<MessageBus> promise = Promise.promise();
    if (INSTANCE == null) {
      synchronized (MessageBusFactory.class) {
        ServiceLoader<MessageBus> services = ServiceLoader.load(MessageBus.class);
        Iterator<MessageBus> it = services.iterator();
        if (!it.hasNext()) {
          throw new IllegalStateException("could not found message bus service! just using default bootstrap");
        } else {
          MessageBus bus = it.next();
          vertx.deployVerticle(bus, ar -> {
            if (ar.succeeded()) {
              INSTANCE = bus;
              promise.complete(INSTANCE);
            } else {
              promise.fail(ar.cause());
            }
          });
        }
      }
    } else {
      promise.complete(INSTANCE);
    }
    return promise.future();
  }
}
