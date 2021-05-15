package com.tqxd.jys.websocket.session;

import com.tqxd.jys.websocket.transport.ServerEndpointVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import org.jctools.queues.MpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 无锁无gc定长会话管理器
 *
 * @author lyqingye
 */
public class SessionManager {
  private static final Logger log = LoggerFactory.getLogger(ServerEndpointVerticle.class);
  private static final long SESSION_CLEAR_TIMER = 1000;
  private static final SessionManager INSTANCE = new SessionManager(1 << 14);
  //
  // bitmap helper
  //
  // 2 ^ 6 = 64 = 8bit * sizeof(long) = 8bit * 8byte = 64
  private final static int ADDRESS_BITS_PER_WORD = 6;
  private final int capacity;
  private final Object[] objects; // must have exact type Object[]
  private AtomicInteger usedCounter = new AtomicInteger(0);
  //
  // channel -> bitmap
  //
  private AtomicLong bitmapVersion = new AtomicLong(0);
  private Map<String, long[]> partition = new ConcurrentHashMap<>();
  private MpmcArrayQueue<Integer> freeQueue;

  public SessionManager(int capacity) {
    if (capacity < 64 || capacity % 8 != 0) {
      throw new IllegalArgumentException("capacity must be >= 64 && capacity % 8 == 0");
    }
    this.capacity = capacity;
    objects = new Object[capacity];
    freeQueue = new MpmcArrayQueue<>(capacity);
    for (int i = 0; i < capacity; i++) {
      Session newSession = new Session(i);
      objects[i] = newSession;
      freeQueue.offer(i);
    }
    startClearExpiredSessionThread();
  }

  public static SessionManager getInstance() {
    return INSTANCE;
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

  private void startClearExpiredSessionThread() {
    Thread thread = new Thread(() -> {
      while (true) {
        for (Object obj : objects) {
          Session session = (Session) obj;
          if (session.tryToExpired()) {
            freeQueue.offer(session.id());
            log.info("[SessionMgr]: clear expired session at id: {}! current number of online session is: {}", session.id(), usedCounter.decrementAndGet());
          }
        }
        try {
          Thread.sleep(SESSION_CLEAR_TIMER);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    thread.setName("session-manager-cleaner-thread");
    thread.start();
  }

  public Session allocate() {
    Integer id;
    for (int i = 0; i < 255; i++) {
      id = freeQueue.poll();
      Session session;
      if (id != null) {
        session = getById(id);
        if (session != null && session.tryToUse()) {
          log.info("[SessionMgr]: allocate session: {}! current number of online session is: {}", session.id(), usedCounter.incrementAndGet());
          return session;
        }
      }
    }
    throw new RuntimeException("get session fail!");
  }

  public boolean release(Session session) {
    if (session.tryToFree()) {
      freeQueue.offer(session.id());
      log.info("[SessionMgr]: release session: {}! current number of online session is: {}", session.id(), usedCounter.decrementAndGet());
      return true;
    }
    return false;
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

  //
  // partition helper functions
  //
  public void foreachSessionByChannel(String ch, Consumer<Session> consumer) {
    long[] bitmap = selectPartition(ch);
    int count = capacity >> ADDRESS_BITS_PER_WORD;
    for (int i = 0; i < count; i++) {
      long word = bitmap[i];
      for (int j = 0; j < 64; j++) {
        boolean isSet = (word & (1L << j)) != 0;
        if (isSet) {
          Session session = (Session) objects[(i << ADDRESS_BITS_PER_WORD) + j];
          if (session != null & consumer != null) {
            try {
              consumer.accept(session);
            } catch (Exception ex) {
              ex.printStackTrace();
            }
          }
        }
      }
    }
  }

  public void removeSessionSubscribedAllChannels(Session session) {
    long currentVersion;
    do {
      currentVersion = bitmapVersion.get();
      if (session == null)
        return;
      int id = session.id();
      for (long[] bitmap : partition.values()) {
        bitmap[id >> ADDRESS_BITS_PER_WORD] &= ~(1L << id);
      }
    } while (!bitmapVersion.compareAndSet(currentVersion, currentVersion + 1));
  }

  public boolean subscribeChannel(Session session, String ch) {
    long currentVersion;
    do {
      currentVersion = bitmapVersion.get();
      long[] bitmap = partition.get(ch);
      if (bitmap == null) {
        return false;
      }
      int id = session.id();
      bitmap[id >> ADDRESS_BITS_PER_WORD] |= (1L << id);
    } while (!bitmapVersion.compareAndSet(currentVersion, currentVersion + 1));
    return true;
  }

  public boolean unSubscribeChannel(Session session, String ch) {
    long currentVersion;
    do {
      currentVersion = bitmapVersion.get();
      long[] bitmap = partition.get(ch);
      if (bitmap == null) {
        return false;
      }
      int id = session.id();
      bitmap[id >> ADDRESS_BITS_PER_WORD] &= ~(1L << id);
    } while (!bitmapVersion.compareAndSet(currentVersion, currentVersion + 1));
    return true;
  }

  private long[] selectPartition(String ch) {
    return partition.computeIfAbsent(ch, k -> new long[capacity >> ADDRESS_BITS_PER_WORD]);
  }


}
