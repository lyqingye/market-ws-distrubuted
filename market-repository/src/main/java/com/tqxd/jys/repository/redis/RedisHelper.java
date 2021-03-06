package com.tqxd.jys.repository.redis;

import com.tqxd.jys.utils.VertxUtil;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.*;
import io.vertx.redis.client.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class RedisHelper {
  /**
   * redis 连接
   */
  private RedisConnection redisConn;

  /**
   * redis api
   */
  private RedisAPI redisApi;

  /**
   * 创建redis仓库
   *
   * @param vertx            vertx
   * @param connectionString 链接字符串
   * @return future
   */
  public static Future<RedisHelper> create(Vertx vertx, String connectionString) {
    return VertxUtil.readJsonFile(vertx, "redis.json").compose(config -> {
      Promise<RedisHelper> promise = Promise.promise();
      RedisHelper self = new RedisHelper();
      Redis.createClient(vertx, new RedisOptions(config)).connect(onConnect -> {
        if (onConnect.succeeded()) {
          self.redisConn = onConnect.result();
          self.redisApi = RedisAPI.api(self.redisConn);
          promise.complete(self);
          // 打印异常堆栈
          self.redisConn.exceptionHandler(Throwable::printStackTrace);
        } else {
          promise.fail(onConnect.cause());
        }
      });
      return promise.future();
    });
  }

  /**
   * Hash get all
   *
   * @param key     key
   * @param handler 返回值处理
   */
  public void hGetAll(String key, Handler<AsyncResult<Map<String, String>>> handler) {
    redisApi.hgetall(key, ar -> {
      if (ar.succeeded()) {
        Response response = ar.result();
        int size = response.size();
        if (size > 0 && size % 2 == 0) {
          Map<String, String> result = new HashMap<>(size >> 1);
          for (int i = 0; i < size; i += 2) {
            // 这里不处理 key 为null的结果
            String k = response.get(i).toString();
            if (k != null) {
              result.put(k, response.get(i + 1).toString());
            }
          }
          handler.handle(Future.succeededFuture(result));
        } else if (size == 0) {
          handler.handle(Future.succeededFuture(Collections.emptyMap()));
        } else {
          handler.handle(Future.failedFuture("invalid response"));
        }
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public <E> void hGet(String key, String hashKey, Handler<AsyncResult<E>> handler) {
    redisApi.hget(key, hashKey, ar -> {
      if (ar.succeeded()) {
        Response response = ar.result();
        handler.handle(Future.succeededFuture(responseToObj(response)));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * Hash set
   *
   * @param key     key
   * @param hashKey hashKey
   * @param value   value
   * @param handler handler
   */
  public void hSet(String key, String hashKey, String value, Handler<AsyncResult<Void>> handler) {
    redisApi.hset(Arrays.asList(key, hashKey, value), ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * SoredSet ZCard
   *
   * @param key     key
   * @param handler handler
   */
  public void zCard(String key, Handler<AsyncResult<Long>> handler) {
    redisApi.zcard(key, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(this.responseToObj(ar.result())));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * SoredSet ZRange
   *
   * @param key     key
   * @param start   start
   * @param stop    stop
   * @param handler handler
   */
  public void zRange(String key, long start, long stop, Handler<AsyncResult<List<String>>> handler) {
    final List<String> cmd = Arrays.asList(key, String.valueOf(start), String.valueOf(stop));
    redisApi.zrange(cmd, ar -> {
      if (ar.succeeded()) {
        final Response response = ar.result();
        final List<String> objects = response.stream().map(obj -> (String) this.responseToObj(obj))
            .collect(Collectors.toList());
        handler.handle(Future.succeededFuture(objects));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void zRangeByScore(String key, double start, double stop, Handler<AsyncResult<List<String>>> handler) {
    final List<String> cmd = Arrays.asList(key, String.valueOf(start), String.valueOf(stop));
    redisApi.zrangebyscore(cmd, ar -> {
      if (ar.succeeded()) {
        final Response response = ar.result();
        final List<String> objects = response.stream().map(obj -> (String) this.responseToObj(obj))
            .collect(Collectors.toList());
        handler.handle(Future.succeededFuture(objects));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * Set sMembers
   *
   * @param key     key
   * @param handler handler
   */
  public void sMembers(String key, Handler<AsyncResult<Set<String>>> handler) {
    redisApi.smembers(key, ar -> {
      if (ar.succeeded()) {
        final Response response = ar.result();
        final Set<String> objects = response.stream().map(obj -> (String) this.responseToObj(obj))
            .collect(Collectors.toSet());
        handler.handle(Future.succeededFuture(objects));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * Set sMembers
   *
   * @param key key
   * @return future
   */
  public Future<Set<String>> sMembers(String key) {
    Promise<Set<String>> promise = Promise.promise();
    this.sMembers(key, promise);
    return promise.future();
  }

  /**
   * 批量命令
   *
   * @param commands 命令
   * @param onSend   结果
   */
  public void batch(List<Request> commands, Handler<AsyncResult<List<@Nullable Response>>> onSend) {
    redisConn.batch(commands, onSend);
  }

  public void set(String key, String value, Handler<AsyncResult<Void>> handler) {
    redisApi.set(Arrays.asList(key, value), ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void get(String key, Handler<AsyncResult<String>> handler) {
    redisApi.get(key, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(responseToObj(ar.result())));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  /**
   * 关闭redis
   */
  public void close() {
    redisApi.close();
  }

  /**
   * 处理消息响应
   *
   * @param r 响应体
   * @return 目标真实对象
   */
  @SuppressWarnings("unchecked")
  private <T> T responseToObj(Response r) {
    if (r == null) {
      return null;
    }
    switch (r.type()) {
      case NUMBER: {
        return (T) Long.valueOf(r.toNumber().longValue());
      }
      case MULTI:
      case BULK: {
        // 统一用String
        return (T) r.toString(StandardCharsets.UTF_8);
      }
      case SIMPLE: {
        return (T) r.toString();
      }
      case ERROR: {
        System.out.println("redis error response, error msg: " + r.toString());
      }
      default: {
        return null;
      }
    }
  }
}
