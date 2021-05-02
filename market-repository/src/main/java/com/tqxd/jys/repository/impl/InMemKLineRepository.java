package com.tqxd.jys.repository.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.disruptor.AbstractDisruptorConsumer;
import com.tqxd.jys.disruptor.DisruptorFactory;
import com.tqxd.jys.disruptor.DisruptorQueue;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.openapi.payload.KlineSnapshotMeta;
import com.tqxd.jys.timeline.KLine;
import com.tqxd.jys.timeline.KLineMeta;
import com.tqxd.jys.timeline.cmd.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;

/**
 * 内存k线仓库，充当缓存
 *
 * @author lyqingye
 */
public class InMemKLineRepository implements KLineRepository {
  private static final long KLINE_TICK_MILLS = 10;
  private static final Logger log = LoggerFactory.getLogger(InMemKLineRepository.class);
  private List<KLineRepositoryListener> listeners = new ArrayList<>();
  private DisruptorQueue<Object> outQueue;
  private ConcurrentLinkedQueue<Object> cmdQueue = new ConcurrentLinkedQueue<>();
  private Map<String,MarketDetailTick> marketDetailCache = new HashMap<>();
  private volatile boolean isRunning = false;
  /**
   * 名称 -> timeLine 映射的索引
   * 用数组和map作为索引，因为需要频繁遍历，避免经常遍历映射导致频繁创建 {@link java.util.Iterator} 迭代器对象
   */
  private Map<String, Integer> indexMap = new HashMap<>();
  private KLine[] timeLines = new KLine[256];
  private int size = 0;

  @Override
  public void open(Vertx vertx,JsonObject config, Handler<AsyncResult<Void>> handler) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
      .setNameFormat("kline-listener-thread-%d")
      .setDaemon(false)
      .setUncaughtExceptionHandler(((t, e) -> e.printStackTrace()))
      .build();
    outQueue = DisruptorFactory.createQueue(1 << 16, threadFactory, disruptorConsumer());
    startJob();
    handler.handle(Future.succeededFuture());
  }

  @Override
  public void restoreWithSnapshot(KlineSnapshot snapshot, Handler<AsyncResult<Void>> handler) {
    RestoreSnapshotCmd cmd = new RestoreSnapshotCmd();
    cmd.setHandler(handler);
    cmd.setSnapshot(snapshot);
    cmdQueue.offer(cmd);
  }

  @Override
  public void append(long commitIndex, String symbol, Period period, KlineTick tick, Handler<AsyncResult<Long>> handler) {
    AppendTickCmd cmd = new AppendTickCmd();
    cmd.setSymbol(symbol);
    cmd.setPeriod(period);
    cmd.setCommitIndex(commitIndex);
    cmd.setTick(tick);
    cmd.setHandler(handler);
    cmdQueue.offer(cmd);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    isRunning = false;
    outQueue.shutdown();
    handler.handle(Future.succeededFuture());
  }

  @Override
  public void query(String symbol, Period period, long from, long to, Handler<AsyncResult<List<KlineTick>>> handler) {
    QueryHistoryCmd cmd = new QueryHistoryCmd();
    cmd.setSymbol(symbol);
    cmd.setPeriod(period);
    cmd.setFrom(from);
    cmd.setTo(to);
    cmd.setHandler(handler);
    cmdQueue.offer(cmd);
  }

  @Override
  public void loadSnapshot(String symbol, Period period, Handler<AsyncResult<KlineSnapshot>> handler) {
    handler.handle(Future.succeededFuture(getOrCreate(symbol,period).snapshot()));
  }

  @Override
  public void addListener(KLineRepositoryListener listener) {
    this.listeners.add(Objects.requireNonNull(listener));
  }

  @Override
  public void getAggregate(String symbol, Handler<AsyncResult<MarketDetailTick>> handler) {
    handler.handle(Future.succeededFuture(marketDetailCache.get(symbol)));
  }

  @Override
  public void putAggregate(String symbol, MarketDetailTick tick, Handler<AsyncResult<Void>> handler) {
    marketDetailCache.put(symbol,tick);
    handler.handle(Future.succeededFuture());
  }

  private AbstractDisruptorConsumer<Object> disruptorConsumer() {
    return new AbstractDisruptorConsumer<Object>() {
      @Override
      public void process(Object event) {
        if (event instanceof AppendTickResult) {
          for (KLineRepositoryListener listener : listeners) {
            listener.onAppendFinished((AppendTickResult) event);
          }
        } else if (event instanceof AutoAggregateResult) {
          for (KLineRepositoryListener listener : listeners) {
            listener.onAutoAggregate((AutoAggregateResult) event);
          }
        }
      }
    };
  }

  private void startJob() {
    isRunning = true;
    Thread thread = new Thread(tickJob());
    thread.setName("kline-auto-aggregate-thread");
    thread.setDaemon(true);
    thread.setUncaughtExceptionHandler(((t, e) -> e.printStackTrace()));
    thread.start();
  }

  /**
   * k线tick job
   *
   * @return 线程
   */
  private Runnable tickJob() {
    return () -> {
      while (isRunning) {
        try {
          // tick k线
          doTickKLines();
          // 处理命令
          Object obj;
          while ((obj = cmdQueue.poll()) != null) {
            if (obj instanceof AppendTickCmd) {
              doAppend((AppendTickCmd) obj);
            } else if (obj instanceof QueryHistoryCmd) {
              doQuery((QueryHistoryCmd) obj);
            } else if (obj instanceof RestoreSnapshotCmd) {
              doRestore((RestoreSnapshotCmd) obj);
            }
          }
          Thread.sleep(KLINE_TICK_MILLS);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    };
  }

  private void doTickKLines() {
    for (int i = 0; i < size; i++) {
      KLine timeLine = timeLines[i];
      if (timeLine != null) {
        MarketDetailTick aggregate = timeLine.tick();
        if (aggregate != null) {
          outQueue.add(new AutoAggregateResult(timeLine.meta().snapshot(), aggregate));
        }
      }
    }
  }

  private void doAppend(AppendTickCmd cmd) {
    getOrCreate(cmd.getSymbol(), cmd.getPeriod())
      .append(cmd.getCommitIndex(), cmd.getTick(), ar -> {
        if (ar.succeeded()) {
          outQueue.add(ar.result());
          cmd.getHandler().handle(Future.succeededFuture(cmd.getCommitIndex()));
        } else {
          cmd.getHandler().handle(Future.failedFuture(ar.cause()));
        }
      });
  }

  private void doQuery(QueryHistoryCmd cmd) {
    getOrCreate(cmd.getSymbol(), cmd.getPeriod())
      .query(cmd.getFrom(), cmd.getTo(), cmd.getHandler());
  }

  private void doRestore(RestoreSnapshotCmd cmd) {
    KlineSnapshotMeta meta = cmd.getSnapshot().getMeta();
    getOrCreate(meta.getSymbol(), meta.getPeriod())
      .restoreWithSnapshot(cmd.getSnapshot(), ar -> {
        if (ar.succeeded()) {
          outQueue.add(ar.result());
          cmd.getHandler().handle(Future.succeededFuture());
        } else {
          cmd.getHandler().handle(Future.failedFuture(ar.cause()));
        }
      });
  }

  private @NonNull KLine getOrCreate(String symbol, Period period) {
    String key = symbol + ":" + period;
    Integer index = indexMap.computeIfAbsent(key, k -> {
      int newSize = ++size;
      resizeIfRequired(newSize);
      timeLines[newSize - 1] = createKLine(symbol, period);
      return newSize - 1;
    });
    return timeLines[index];
  }

  private void resizeIfRequired(int newSize) {
    if (newSize > timeLines.length) {
      KLine[] newKLines = new KLine[timeLines.length << 1];
      System.arraycopy(timeLines, 0, newKLines, 0, timeLines.length);
      timeLines = newKLines;
    }
  }

  private KLine createKLine(String symbol, Period period) {
    KLineMeta meta = new KLineMeta();
    meta.setSymbol(symbol);
    meta.setPeriod(period);
    return new KLine(meta, period, period.equals(Period._1_MIN));
  }
}
