package com.tqxd.jys.timeline;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.disruptor.AbstractDisruptorConsumer;
import com.tqxd.jys.disruptor.DisruptorQueue;
import com.tqxd.jys.disruptor.DisruptorFactory;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.openapi.payload.KlineSnapshotMeta;
import com.tqxd.jys.timeline.cmd.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * k线数据管理器
 *
 * @author lyqingye
 */
public class KLineManager {
  private ConcurrentLinkedQueue<Object> inCmdQueue = new ConcurrentLinkedQueue<>();
  private DisruptorQueue<Object> outResultQueue;
  private Consumer<Object> consumer;
  /**
   * 名称 -> timeLine 映射的索引
   * 用数组和map作为索引，因为需要频繁遍历，避免经常遍历映射导致频繁创建 {@link java.util.Iterator} 迭代器对象
   */
  private Map<String, Integer> indexMap = new HashMap<>();
  private KLine[] timeLines = new KLine[256];
  private int size = 0;

  public static KLineManager create() {
    KLineManager mgr = new KLineManager();
    mgr.outResultQueue = DisruptorFactory.createQueue(1 << 16, mgr.createThreadFactory("kline-output-result-thread-"), mgr.adapterConsumer());
    mgr.startJob();
    return mgr;
  }

  public void setOutResultConsumer(Consumer<Object> consumer) {
    this.consumer = consumer;
  }

  /**
   * apply tick到指定k线
   *
   * @param committedIndex 消息索引
   * @param tick           tick
   * @return {@link AppendTickResult} 不为null
   */
  public void applyTick(String symbol, Period period, long committedIndex, @NonNull KlineTick tick,
                        @NonNull Handler<AsyncResult<Long>> handler) {
    AppendTickCmd cmd = new AppendTickCmd();
    cmd.setSymbol(symbol);
    cmd.setPeriod(period);
    cmd.setCommitIndex(committedIndex);
    cmd.setTick(tick);
    cmd.setHandler(handler);
    inCmdQueue.offer(cmd);
  }

  public void pollTicks(String symbol, Period period, long from, long to,
                        @NonNull Handler<AsyncResult<List<KlineTick>>> handler) {
    QueryHistoryCmd cmd = new QueryHistoryCmd();
    cmd.setSymbol(symbol);
    cmd.setPeriod(period);
    cmd.setFrom(from);
    cmd.setTo(to);
    cmd.setHandler(handler);
    inCmdQueue.offer(cmd);
  }

  public void applySnapshot(KlineSnapshot snapshot,
                            @NonNull Handler<AsyncResult<Void>> handler) {
    RestoreSnapshotCmd cmd = new RestoreSnapshotCmd();
    cmd.setSnapshot(snapshot);
    cmd.setHandler(handler);
    inCmdQueue.offer(cmd);
  }

  private void startJob() {
    Thread thread = new Thread(tickJob());
    thread.setName("kline-timeLine-tick-thread]");
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
      while (true) {
        try {
          // tick k线
          tickKLines();
          // 处理命令
          Object obj;
          while ((obj = inCmdQueue.poll()) != null) {
            if (obj instanceof AppendTickCmd) {
              execApplyTickCmd((AppendTickCmd) obj);
            } else if (obj instanceof QueryHistoryCmd) {
              execPollTicksCmd((QueryHistoryCmd) obj);
            } else if (obj instanceof RestoreSnapshotCmd) {
              execApplySnapshotCmd((RestoreSnapshotCmd) obj);
            }
          }
          Thread.sleep(10);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    };
  }

  private void tickKLines() {
    // tick 所有k线
    for (int i = 0; i < size; i++) {
      KLine timeLine = timeLines[i];
      if (timeLine != null) {
        // tick当前k线
        MarketDetailTick aggregate = timeLine.tick();
        // k线窗口滑动，触发了数据聚合
        if (aggregate != null) {
          outResultQueue.add(new AutoAggregateResult(timeLine.meta().snapshot(),aggregate));
        }
      }
    }
  }

  private void execApplyTickCmd(AppendTickCmd cmd) {
    selectKline(cmd.getSymbol(), cmd.getPeriod())
        .append(cmd.getCommitIndex(), cmd.getTick(), ar -> {
          if (ar.succeeded()) {
            outResultQueue.add(ar.result());
            cmd.getHandler().handle(Future.succeededFuture(cmd.getCommitIndex()));
          } else {
            cmd.getHandler().handle(Future.failedFuture(ar.cause()));
          }
        });
  }

  private void execPollTicksCmd(QueryHistoryCmd cmd) {
    selectKline(cmd.getSymbol(), cmd.getPeriod()).query(cmd.getFrom(), cmd.getTo(), cmd.getHandler());
  }

  private void execApplySnapshotCmd(RestoreSnapshotCmd cmd) {
    KlineSnapshotMeta meta = cmd.getSnapshot().getMeta();
    selectKline(meta.getSymbol(), meta.getPeriod())
        .restoreWithSnapshot(cmd.getSnapshot(), ar -> {
          if (ar.succeeded()) {
            outResultQueue.add(ar.result());
            cmd.getHandler().handle(Future.succeededFuture());
          } else {
            cmd.getHandler().handle(Future.failedFuture(ar.cause()));
          }
        });
  }

  private @NonNull KLine selectKline(String symbol, Period period) {
    String key = symbol + ":" + period;
    Integer index = indexMap.computeIfAbsent(key, k -> {
      KLineMeta meta = new KLineMeta();
      meta.setSymbol(symbol);
      meta.setPeriod(period);
      KLine timeLine = new KLine(meta, period, period.equals(Period._1_MIN));
      int newSize = ++size;
      if (newSize > timeLines.length) {
        KLine[] newKLines = new KLine[timeLines.length << 1];
        System.arraycopy(timeLines, 0, newKLines, 0, timeLines.length);
        timeLines = newKLines;
      }
      timeLines[newSize - 1] = timeLine;
      return newSize - 1;
    });
    return timeLines[index];
  }

  private ThreadFactory createThreadFactory(@NonNull String namePrefix) {
    return new ThreadFactory() {
      AtomicInteger counter = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(namePrefix + counter.getAndIncrement());
        thread.setDaemon(false);
        thread.setUncaughtExceptionHandler(((t, e) -> e.printStackTrace()));
        return thread;
      }
    };
  }

  private @NonNull AbstractDisruptorConsumer<Object> adapterConsumer() {
    return new AbstractDisruptorConsumer<Object>() {
      @Override
      public void process(Object event) {
        if (consumer != null) {
          consumer.accept(event);
        }
      }
    };
  }
}
