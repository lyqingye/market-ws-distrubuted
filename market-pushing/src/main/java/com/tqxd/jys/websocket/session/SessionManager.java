package com.tqxd.jys.websocket.session;

import com.tqxd.jys.utils.VertxUtil;
import com.tqxd.jys.websocket.transport.ServerEndpointVerticle;
import io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.jctools.queues.MpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 无锁无gc定长会话管理器
 *
 * @author lyqingye
 */
public class SessionManager {
  private static final String MAX_SESSIONS_LIMIT_CONFIG = "market.pushing.websocket.max-session-limit";
  private static final Logger log = LoggerFactory.getLogger(ServerEndpointVerticle.class);
  private static final long SESSION_CLEAR_TIMER = 1000;
  private static volatile SessionManager INSTANCE;
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
  private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
  private static final long BITMAP_VERSION_ARRAY_BASE;
  private static final int BITMAP_VERSION_ARRAY_SHIFT;

  static {
    BITMAP_VERSION_ARRAY_BASE = UNSAFE.arrayBaseOffset(long[].class);
    BITMAP_VERSION_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(UNSAFE.arrayIndexScale(long[].class));
  }

  private Map<String, long[]> partition = new ConcurrentHashMap<>();
  private MpmcArrayQueue<Integer> freeQueue;

  private volatile long[] bitmapVersions;

  public SessionManager(int capacity) {
    if (capacity < 64 || capacity % 8 != 0) {
      throw new IllegalArgumentException("capacity must be >= 64 && capacity % 8 == 0");
    }
    this.capacity = capacity;
    objects = new Object[capacity];
    bitmapVersions = new long[capacity];
    freeQueue = new MpmcArrayQueue<>(capacity);
    for (int i = 0; i < capacity; i++) {
      Session newSession = new Session(i);
      objects[i] = newSession;
      freeQueue.offer(i);
      bitmapVersions[i] = 0L;
    }

    startClearExpiredSessionThread();
  }

  public static SessionManager getInstance(JsonObject config) {
    synchronized (SessionManager.class) {
      if (INSTANCE == null) {
        INSTANCE = new SessionManager(VertxUtil.jsonGetValue(config, MAX_SESSIONS_LIMIT_CONFIG, Integer.class, 4096));
      }
    }
    return INSTANCE;
  }

  public int getCapacity() {
    return this.capacity;
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
            log.info("[SessionMgr]: clear expired session at id: {}! current number of online session is: {}",
                session.id(), usedCounter.decrementAndGet());
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
    Integer id = freeQueue.poll();
    Session session;
    if (id != null) {
      session = getById(id);
      if (session != null && session.tryToUse()) {
        log.info("[SessionMgr]: allocate session: {}! current number of online session is: {}", session.id(),
            usedCounter.incrementAndGet());
        return session;
      }
    }
    return null;
  }

  public boolean release(Session session) {
    if (session.tryToFree()) {
      freeQueue.offer(session.id());
      log.info("[SessionMgr]: release session: {}! current number of online session is: {}", session.id(),
          usedCounter.decrementAndGet());
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
    for (int i = 0; i < bitmap.length; i++) {
      long word = bitmap[i];
      // 1Long = 8byte = 64bit
      for (int j = 0; j < 64; j++) {
        boolean isSet = (word & (1L << j)) != 0;
        if (isSet) {
          // sessionIndex = i * 64 + j
          // i << 6 = i * 64
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
    if (session == null)
      return;
    long currentVersion;
    do {
      currentVersion = getBitmapVersion(session.id());
      int id = session.id();
      for (long[] bitmap : partition.values()) {
        bitmap[id >> ADDRESS_BITS_PER_WORD] &= ~(1L << id);
      }
    } while (!compareAndSetBitmapVersion(session.id(), currentVersion, currentVersion + 1));
  }

  public boolean subscribeChannel(Session session, String ch) {
    long currentVersion;
    do {
      currentVersion = getBitmapVersion(session.id());
      long[] bitmap = partition.get(ch);
      if (bitmap == null) {
        return false;
      }
      int id = session.id();
      bitmap[id >> ADDRESS_BITS_PER_WORD] |= (1L << id);
    } while (!compareAndSetBitmapVersion(session.id(), currentVersion, currentVersion + 1));
    return true;
  }

  public boolean unSubscribeChannel(Session session, String ch) {
    long currentVersion;
    do {
      currentVersion = getBitmapVersion(session.id());
      long[] bitmap = partition.get(ch);
      if (bitmap == null) {
        return false;
      }
      int id = session.id();
      bitmap[id >> ADDRESS_BITS_PER_WORD] &= ~(1L << id);
    } while (!compareAndSetBitmapVersion(session.id(), currentVersion, currentVersion + 1));
    return true;
  }

  private long[] selectPartition(String ch) {
    return partition.computeIfAbsent(ch, k -> new long[capacity >> ADDRESS_BITS_PER_WORD]);
  }

  private long getBitmapVersion(long sessionId) {
    return (long) UNSAFE.getLongVolatile(bitmapVersions,
        ((long) sessionId << BITMAP_VERSION_ARRAY_SHIFT) + BITMAP_VERSION_ARRAY_BASE);
  }

  private boolean compareAndSetBitmapVersion(long sessionId, long except, long update) {
    return UNSAFE.compareAndSwapLong(bitmapVersions,
        ((long) sessionId << BITMAP_VERSION_ARRAY_SHIFT) + BITMAP_VERSION_ARRAY_BASE, except, update);
  }
}
