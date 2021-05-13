package com.tqxd.jys.timeline;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.constance.Period;
import com.tqxd.jys.openapi.payload.KlineSnapshot;
import com.tqxd.jys.openapi.payload.KlineSnapshotMeta;
import com.tqxd.jys.timeline.cmd.AppendTickResult;
import com.tqxd.jys.timeline.cmd.Auto24HourStatisticsResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.tqxd.jys.utils.TimeUtils.alignWithPeriod;

/**
 * k线时间线
 *
 * @author lyqingye
 */
public class KLine {
  /**
   * 周期大小
   */
  private final long period;
  /**
   * 周期数
   */
  private final int numOfPeriod;
  /**
   * 总周期
   */
  private final long totalPeriodSize;
  /**
   * 时间轮创建的时间
   */
  private long tt;
  /**
   * 时间轮创建时头部对应的时间
   */
  private long ht;
  /**
   * 时间轮存放的具体数据
   */
  private Object[] data;
  /**
   * 元数据
   */
  private KLineMeta meta;

  public KLine(KLineMeta meta, Period p) {
    this.meta = meta;
    this.period = p.getMill();
    this.numOfPeriod = p.getNumOfPeriod();
    this.totalPeriodSize = p.getMill() * numOfPeriod;
    long now = nowBeijingTimeStamp();
    this.tt = alignWithPeriod(now, p.getMill());
    this.data = new Object[numOfPeriod];
    this.ht = tt - totalPeriodSize + p.getMill();
  }

  public KLineMeta meta() {
    return this.meta;
  }

  /**
   * k线拉取指定指定时间范围内的数据
   *
   * @param from 开始时间
   * @param to   结束时间
   */
  public void query(long from, long to, Handler<AsyncResult<List<KlineTick>>> handler) {
    int startIdx = calculateIdx(alignWithPeriod(from + TimeUnit.HOURS.toSeconds(8), period));
    int endIdx = calculateIdx(alignWithPeriod(to + TimeUnit.HOURS.toSeconds(8), period));

    if (startIdx <= 0 || startIdx >= numOfPeriod) {
      startIdx = 0;
    }
    if (endIdx < 0 || endIdx >= numOfPeriod) {
      endIdx = numOfPeriod - 1;
    }
    List<KlineTick> result = new ArrayList<>(numOfPeriod);
    while (startIdx <= endIdx) {
      KlineTick obj = (KlineTick) this.data[startIdx];
      if (obj != null) {
        if (obj.getId() >= from && obj.getId() <= to) {
          result.add(obj);
        }
      }
      startIdx++;
    }
    handler.handle(Future.succeededFuture(result));
  }

  /**
   * 应用一个tick
   *
   * @param commitIndex 消息索引
   * @param newObj      tick
   */
  public void append(long commitIndex, KlineTick newObj, Handler<AsyncResult<AppendTickResult>> handler) {
    if (commitIndex <= meta.getCommitIndex()) {
      handler.handle(Future.failedFuture("invalid commit index cur commitIndex: " + meta.getCommitIndex() + " cmd commitIndex: " + commitIndex));
      return;
    }
    KlineTick updateTick = append(newObj);
    if (updateTick == null) {
      handler.handle(Future.failedFuture("apply tick fail! index outbound: " + calculateIdx(newObj.getTime())));
      return;
    }
    // complete
    handler.handle(Future.succeededFuture(new AppendTickResult(meta.snapshot(), updateTick, get24HourStatistics())));
    // apply the committed index
    meta.applyCommittedIndex(commitIndex);
  }

  public KlineSnapshot snapshot () {
    KlineSnapshot snapshot = new KlineSnapshot();
    KlineSnapshotMeta meta = new KlineSnapshotMeta();
    meta.setTs(nowBeijingTimeStamp());
    meta.setSymbol(this.meta.getSymbol());
    meta.setCommittedIndex(this.meta.getCommitIndex());
    meta.setPeriod(this.meta.getPeriod());
    snapshot.setMeta(meta);
    List<KlineTick> copy = new ArrayList<>(data.length);
    for (Object obj : data) {
      if (obj != null) {
        copy.add((KlineTick) obj);
      }
    }
    snapshot.setTickList(copy);
    return snapshot;
  }

  /**
   * 应用快照
   */
  public void restoreWithSnapshot(KlineSnapshot snapshot, Handler<AsyncResult<Auto24HourStatisticsResult>> handler) {
    KlineSnapshotMeta meta = snapshot.getMeta();
    if (meta.getCommittedIndex() < 0) {
      handler.handle(Future.failedFuture("invalid commit index while apply the snapshot! commit index: " + meta.getCommittedIndex()));
      return;
    }
    for (KlineTick tick : snapshot.getTickList()) {
      append(tick);
    }
    this.meta.applyCommittedIndex(meta.getCommittedIndex());
    handler.handle(Future.succeededFuture(new Auto24HourStatisticsResult(this.meta.snapshot(), get24HourStatistics())));
  }

  public KlineTick tick() {
    KlineTick result = null;
    if (updateWindow()) {
      result = get24HourStatistics();
    }
    return result;
  }

  private KlineTick append(KlineTick source) {
    KlineTick newObj = source.deepClone();
    int idx = calculateIdx(newObj.getTime());
    if (idx < 0 || idx >= numOfPeriod) {
      return null;
    }
    KlineTick oldObj = (KlineTick) data[idx];
    if (oldObj != null) {
      boolean isInSamePeriod = alignWithPeriod(newObj.getTime(), period) == alignWithPeriod(oldObj.getTime(), period);
      if (isInSamePeriod) {
        data[idx] = oldObj.merge(newObj);
      } else {
        data[idx] = newObj;
      }
    } else {
      data[idx] = newObj;
    }
    return (KlineTick) data[idx];
  }

  private boolean updateWindow() {
    long now = alignWithPeriod(nowBeijingTimeStamp(), period);
    int roteCount = Math.toIntExact((now - tt) / period);
    if (roteCount != 0) {
      if (roteCount < numOfPeriod) {
        int sPos = roteCount;
        int dPos = 0;
        int length = numOfPeriod - roteCount;
        for (int i = 0; i < length; i++) {
          this.data[dPos] = this.data[sPos];
          // clear src data
          this.data[sPos] = null;
          dPos++;
          sPos++;
        }
      } else {
        Arrays.fill(this.data, null);
      }
      ht += period * roteCount;
      tt += period * roteCount;
      return true;
    } else {
      // the window already updated
      return false;
    }
  }

//  private void clearAggregate() {
//    if (!autoAggregate)
//      return;
//    low = high = vol = open = close = amount = BigDecimal.ZERO;
//    count = 0;
//  }

//  @Deprecated
//  private void doAggregate(KlineTick tick) {
//    if (!autoAggregate || tick == null) {
//      return;
//    }
//    count += tick.getCount();
//    amount = amount.add(tick.getAmount());
//    vol = vol.add(tick.getVol());
//    close = tick.getClose();
//    if (open.compareTo(BigDecimal.ZERO) == 0) {
//      open = tick.getOpen();
//    }
//    close = tick.getClose();
//    if (tick.getHigh().compareTo(high) > 0) {
//      high = tick.getHigh();
//    }
//    if (tick.getLow().compareTo(low) > 0) {
//      low = tick.getLow();
//    }
//  }
//
//  @Deprecated
//  private void doRollbackAggregate(KlineTick oldTick) {
//    if (!autoAggregate || oldTick == null) {
//      return;
//    }
//    count -= oldTick.getCount();
//    amount = amount.subtract(oldTick.getAmount());
//    vol = vol.subtract(oldTick.getVol());
//  }
//
//  @Deprecated
//  private MarketDetailTick snapAggregate() {
//    if (!autoAggregate) {
//      return null;
//    }
//    MarketDetailTick detail = new MarketDetailTick();
//    detail.setVol(vol);
//    detail.setAmount(amount);
//    detail.setClose(close);
//    detail.setOpen(open);
//    detail.setCount(count);
//    detail.setHigh(high);
//    detail.setLow(low);
//    return detail;
//  }

  private int calculateIdx(long t) {
    long at = alignWithPeriod(t, period);
    return Math.toIntExact(((at - ht) % totalPeriodSize) / period);
  }

  private KlineTick get24HourStatistics() {
    if (Period._1_DAY.getMill() != period) {
      return null;
    }
    int idx = calculateIdx(nowBeijingTimeStamp());
    if (idx < 0 || idx >= data.length) {
      return null;
    }
    KlineTick tick = (KlineTick) data[idx];
    if (tick == null) {
      tick = new KlineTick();
      tick.setId(nowBeijingTimeStamp());
      tick.setAmount(BigDecimal.ZERO);
      tick.setClose(BigDecimal.ZERO);
      tick.setOpen(BigDecimal.ZERO);
      tick.setHigh(BigDecimal.ZERO);
      tick.setCount(0);
      tick.setVol(BigDecimal.ZERO);
    }
    return tick;
  }

  private long nowBeijingTimeStamp() {
    return System.currentTimeMillis() / 1000 + TimeUnit.HOURS.toSeconds(8);
  }
}
