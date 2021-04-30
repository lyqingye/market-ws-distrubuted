package com.tqxd.jys.websocket.session;

import com.tqxd.jys.websocket.ServerEndpoint;
import io.netty.util.internal.shaded.org.jctools.queues.atomic.MpscAtomicArrayQueue;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * 无锁无gc定长会话管理器
 *
 * @author lyqingye
 */
public class FastSessionMgr {
  private static final Logger log = LoggerFactory.getLogger(ServerEndpoint.class);
  private final Object[] objects; // must have exact type Object[]
  private MpscAtomicArrayQueue<Object> freeQueue;
  private AtomicInteger usedCounter = new AtomicInteger(0);

  public FastSessionMgr(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be > 0");
    }
    objects = new Object[capacity];
    freeQueue = new MpscAtomicArrayQueue<>(capacity);
    for (int i = 0; i < capacity; i++) {
      Session newSession = new Session(i);
      objects[i] = newSession;
      freeQueue.offer(newSession);
    }
    startClearExpiredSessionThread();
  }

  public Session getById(int id) {
    if (id < 0 || id >= objects.length)
      throw new IllegalArgumentException("id " + id);
    Session session = (Session) objects[id];
    if (session == null) {
      return null;
    }
    if (session.id() != id) {
      throw new IllegalStateException("invalid session object by id: " + id);
    }
    return session;
  }

  public void startClearExpiredSessionThread() {
    new Thread(() -> {
      while (true) {
        for (Object obj : objects) {
          Session session = (Session) obj;
          if (session.tryToExpired()) {
            usedCounter.decrementAndGet();
            freeQueue.offer(session);
            log.info("[SessionMgr]: clear expired session at id: {}", session.id());
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }).start();
  }

  public Session allocate() {
    Session session;
    for (int i = 0; i < 255; i++) {
      session = (Session) freeQueue.poll();
      if (session != null && session.tryToUse()) {
        usedCounter.incrementAndGet();
        return session;
      }
    }
    throw new RuntimeException("get session fail!");
  }

  public boolean release(Session session) {
    if (session.tryToFree()) {
      usedCounter.decrementAndGet();
      freeQueue.offer(session);
      return true;
    }
    return false;
  }

  public void broadcastText(String text) {
    broadcastText(text, null);
  }

  public void broadcastText(String text, Predicate<Session> isSend) {
    for (Object object : objects) {
      Session session = (Session) object;
      if (session.state == Session.USED) {
        if (isSend != null) {
          if (isSend.test(session)) {
            session.writeText(text);
          }
        } else {
          session.writeText(text);
        }
      }
    }
  }

  public void broadcast(Buffer buffer) {
    broadcast(buffer, null);
  }

  public void broadcast(Buffer buffer, Predicate<Session> isSend) {
    for (Object object : objects) {
      Session session = (Session) object;
      if (session.state == Session.USED) {
        if (isSend != null) {
          if (isSend.test(session)) {
            session.write(buffer);
          }
        } else {
          session.write(buffer);
        }
      }
    }
  }

  public void broadcastBinary(Buffer buffer) {
    broadcastBinary(buffer, null);
  }

  public void broadcastBinary(Buffer buffer, Predicate<Session> isSend) {
    for (Object object : objects) {
      Session session = (Session) object;
      if (session.state == Session.USED) {
        if (isSend != null) {
          if (isSend.test(session)) {
            session.writeBinary(buffer);
          }
        } else {
          session.writeBinary(buffer);
        }
      }
    }
  }

  public Session toSession(ServerWebSocket client) {
    Integer sessionId = Session.getSessionId(client);
    if (sessionId != null) {
      return getById(sessionId);
    }
    return null;
  }

  public void refreshTTL(ServerWebSocket client, long expire, TimeUnit timeUnit) {
    Integer sessionId = Session.getSessionId(client);
    if (sessionId != null) {
      Session session = getById(sessionId);
      session.refreshTTL(expire, timeUnit);
    }
  }
}
