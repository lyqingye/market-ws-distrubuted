package com.tqxd.jys.websocket.session;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.impl.WebSocketImplBase;
import sun.misc.Unsafe;

import java.util.concurrent.TimeUnit;

/**
 * 会话
 *
 * @author lyqingye
 */
@SuppressWarnings("unchecked")
public class Session {
  // 空闲
  public static final int FREE = 0;
  // 使用中
  public static final int USED = 1;
  // 过期清理中
  public static final int EXPIRED = -1;
  public static final AttributeKey<Integer> SESSION_ID_IN_CLIENT = AttributeKey.valueOf("_session_id");
  private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
  private static final long STATE_OFFSET, CLIENT_OFFSET, CTX_OFFSET, TTL_OFFSET;
  private static final long CTX_OFFSET_OF_CLIENT;

  static {
    STATE_OFFSET = UnsafeAccess.fieldOffset(Session.class, "state");
    CLIENT_OFFSET = UnsafeAccess.fieldOffset(Session.class, "client");
    CTX_OFFSET = UnsafeAccess.fieldOffset(Session.class, "ctx");
    TTL_OFFSET = UnsafeAccess.fieldOffset(Session.class, "ttl");
    CTX_OFFSET_OF_CLIENT = UnsafeAccess.fieldOffset(WebSocketImplBase.class, "chctx");
  }

  private final int id;
  protected volatile int state = FREE;
  private volatile ServerWebSocket client = null;
  private volatile ChannelHandlerContext ctx = null;
  private volatile long ttl = -1;

  public Session(int id) {
    this.id = id;
  }

  protected static Integer getSessionId(ServerWebSocket client) {
    ChannelHandlerContext ctx = (ChannelHandlerContext) UNSAFE.getObject(client, CTX_OFFSET_OF_CLIENT);
    if (ctx != null) {
      return ctx.channel().attr(SESSION_ID_IN_CLIENT).get();
    }
    return null;
  }

  protected boolean tryToUse() {
    return UNSAFE.compareAndSwapInt(this, STATE_OFFSET, FREE, USED);
  }

  protected boolean tryToExpired() {
    if (ttl != -1 && System.currentTimeMillis() >= ttl) {
      if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, USED, EXPIRED)) {
        client.close().onFailure(Throwable::printStackTrace);
        UNSAFE.putOrderedLong(this, TTL_OFFSET, 0);
        UNSAFE.putOrderedObject(this, CLIENT_OFFSET, null);
        UNSAFE.putOrderedObject(this, CTX_OFFSET, null);
        state = FREE;
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  protected boolean tryToFree() {
    // 避免设置状态为 FREE 后该会话立马被使用
    ServerWebSocket refClient = this.client;
    if (UNSAFE.compareAndSwapInt(this, STATE_OFFSET, USED, FREE)) {
      if (refClient != null) {
        refClient.close().onFailure(Throwable::printStackTrace);
      }
      ttl = -1;
      ctx = null;
      client = null;
      return true;
    }
    return false;
  }

  public void initSession(ServerWebSocket client, long expire, TimeUnit timeUnit) {
    this.client = client;
    this.ctx = (ChannelHandlerContext) UNSAFE.getObject(client, CTX_OFFSET_OF_CLIENT);
    refreshTTL(expire, timeUnit);

    // channel 设置id
    ctx.channel().attr(SESSION_ID_IN_CLIENT).set(id);
  }

  public void refreshTTL(long expire, TimeUnit timeUnit) {
    if (expire == -1) {
      this.ttl = -1;
    } else {
      this.ttl = System.currentTimeMillis() + timeUnit.toMillis(expire);
    }
  }

  public <T> T getAttr(String key) {
    return (T) ctx.channel().attr(AttributeKey.valueOf(key)).get();
  }

  public void setAttr(String key, Object value) {
    ctx.channel().attr(AttributeKey.valueOf(key)).set(value);
  }

  public void hasAttr(String key) {
    ctx.channel().hasAttr(AttributeKey.valueOf(key));
  }

  public <T> T getAttr(AttributeKey<T> key) {
    return (T) ctx.channel().attr(key).get();
  }

  public <T> void setAttr(AttributeKey<T> key, T value) {
    ctx.channel().attr(key).set(value);
  }

  public <T> void hasAttr(AttributeKey<T> key) {
    ctx.channel().hasAttr(key);
  }

  public int id() {
    return id;
  }

  public void writeText(String text) {
    if (state == USED) {
      client.writeTextMessage(text);
    }
  }

  public void write(Buffer buffer) {
    if (state == USED) {
      client.write(buffer);
    }
  }

  public void writeBinary(Buffer buffer) {
    if (state == USED) {
      client.writeBinaryMessage(buffer);
    }
  }
}
