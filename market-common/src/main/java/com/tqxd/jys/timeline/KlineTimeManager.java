package com.tqxd.jys.timeline;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.timeline.cmd.CmdResult;
import com.tqxd.jys.timeline.cmd.UpdateTickResult;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.Vertx;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * k线管理器
 *
 * @author lyqingye
 */
public class KlineTimeManager {
  /**
   * 名称 -> timeLine 映射的索引
   * 用数组和map作为索引，因为需要频繁遍历，避免经常遍历映射导致频繁创建 {@link java.util.Iterator} 迭代器对象
   */
  private Map<String, Integer> timeLineIndexMap = new ConcurrentHashMap<>();
  private KlineTimeLine[] timeLines = new KlineTimeLine[256];
  private AtomicInteger timeLinesSize = new AtomicInteger(0);

  /**
   * 聚合数据消费者
   * String -> k线名称
   * MarketDetailTick -> 聚合数据
   */
  private BiConsumer<KlineTimeLineMeta, MarketDetailTick> aggregateConsumer;

  /**
   * vertx
   */
  private Vertx vertx;

  private KlineTimeManager() {
  }

  public static KlineTimeManager create(Vertx vertx, BiConsumer<KlineTimeLineMeta, MarketDetailTick> aggregateConsumer) {
    KlineTimeManager mgr = new KlineTimeManager();
    mgr.vertx = Objects.requireNonNull(vertx);
    mgr.aggregateConsumer = Objects.requireNonNull(aggregateConsumer);
    mgr.startTickKlineThread();
    return mgr;
  }

  public CmdResult<UpdateTickResult> applyTick(String klineKey, Period period, long commitIndex, KlineTick tick) {
    KlineTimeLine timeLine = getOrCreate(klineKey, period);
    return timeLine.update(commitIndex, tick);
  }

  public void pollTicks() {

  }

  public void applySnapshot() {

  }

  public KlineTimeLine getOrCreate(String klineKey, Period period) {
    Integer index = timeLineIndexMap.computeIfAbsent(klineKey, k -> {
      KlineTimeLineMeta meta = new KlineTimeLineMeta();
      meta.setKlineKey(klineKey);
      KlineTimeLine timeLine = new KlineTimeLine(meta, period.getMill(), period.getNumOfPeriod(), period.equals(Period._1_MIN));
      int newSize = timeLinesSize.incrementAndGet();
      if (newSize > timeLines.length) {
        KlineTimeLine[] newKlineTimeLines = new KlineTimeLine[timeLines.length << 1];
        System.arraycopy(timeLines, 0, newKlineTimeLines, 0, timeLines.length);
        timeLines = newKlineTimeLines;
      }
      timeLines[newSize - 1] = timeLine;
      return newSize - 1;
    });
    return timeLines[index];
  }

  private void startTickKlineThread() {
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
        // tick 所有k线
        for (int i = 0; i < timeLinesSize.get(); i++) {
          KlineTimeLine timeLine = timeLines[i];
          if (timeLine != null) {
            // tick当前k线
            MarketDetailTick aggregate = timeLine.tick();
            // k线窗口滑动，触发了数据聚合
            if (aggregate != null) {
              // 异步消费聚合数据
              VertxUtil.asyncFastCallIgnoreRs(vertx, () -> {
                aggregateConsumer.accept(timeLine.meta(), aggregate);
              });
            }
          }
        }
        try {
          Thread.sleep(0);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
  }
}
