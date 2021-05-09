package com.tqxd.jys.core.kline;

import com.tqxd.jys.core.kline.cmd.AppendTickResult;
import com.tqxd.jys.core.kline.cmd.AutoAggregateResult;
import com.tqxd.jys.core.message.detail.MarketDetailTick;
import com.tqxd.jys.core.message.kline.KlineSnapshot;
import com.tqxd.jys.core.message.kline.KlineSnapshotMeta;
import com.tqxd.jys.core.message.kline.KlineTick;
import com.tqxd.jys.core.message.kline.Period;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
   * 统计项
   */
  private boolean autoAggregate = false;
  private BigDecimal high = BigDecimal.ZERO, low = BigDecimal.ZERO, vol = BigDecimal.ZERO, open = BigDecimal.ZERO, close = BigDecimal.ZERO, amount = BigDecimal.ZERO;
  private int count = 0;

  /**
   * 元数据
   */
  private KLineMeta meta;

  public KLine(KLineMeta meta, Period p, boolean autoAggregate) {
    this.meta = meta;
    this.period = p.getMill();
    this.numOfPeriod = p.getNumOfPeriod();
    this.totalPeriodSize = p.getMill() * numOfPeriod;
    long now = System.currentTimeMillis();
    this.tt = alignWithPeriod(now, p.getMill());
    this.data = new Object[numOfPeriod];
    this.ht = tt - totalPeriodSize + p.getMill();
    this.autoAggregate = autoAggregate;
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
    int startIdx = calculateIdx(alignWithPeriod(from, period));
    int endIdx = calculateIdx(alignWithPeriod(to, period));
    if (startIdx >= 0 && startIdx < numOfPeriod && endIdx > startIdx) {
      endIdx = Math.min(endIdx, numOfPeriod - 1);
    } else {
      handler.handle(Future.failedFuture("fail to poll kline data! from: " + from + " to: " + to + " startIdx: " + startIdx + " endIdx: " + endIdx));
      return;
    }
    List<KlineTick> result = new ArrayList<>(300);
    while (startIdx <= endIdx) {
      KlineTick obj = (KlineTick) this.data[startIdx];
      if (obj != null) {
        if (obj.getTime() >= from && obj.getTime() <= to) {
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
    // aggregate the window
    doAggregate(newObj);
    // complete
    handler.handle(Future.succeededFuture(new AppendTickResult(meta.snapshot(), updateTick, snapAggregate())));
    // apply the committed index
    meta.applyCommittedIndex(commitIndex);
  }

  public KlineSnapshot snapshot() {
    KlineSnapshot snapshot = new KlineSnapshot();
    KlineSnapshotMeta meta = new KlineSnapshotMeta();
    meta.setTs(System.currentTimeMillis());
    meta.setSymbol(meta.getSymbol());
    meta.setCommittedIndex(meta.getCommittedIndex());
    meta.setPeriod(meta.getPeriod());
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
  public void restoreWithSnapshot(KlineSnapshot snapshot, Handler<AsyncResult<AutoAggregateResult>> handler) {
    KlineSnapshotMeta meta = snapshot.getMeta();
    if (meta.getCommittedIndex() < 0) {
      handler.handle(Future.failedFuture("invalid commit index while apply the snapshot! commit index: " + meta.getCommittedIndex()));
      return;
    }
    for (KlineTick tick : snapshot.getTickList()) {
      KlineTick newObj = append(tick);
      if (newObj != null) {
        doAggregate(newObj);
      }
    }
    this.meta.applyCommittedIndex(meta.getCommittedIndex());
    handler.handle(Future.succeededFuture(new AutoAggregateResult(this.meta.snapshot(), snapAggregate())));
  }

  public MarketDetailTick tick() {
    MarketDetailTick result = null;
    if (updateWindow()) {
      if (autoAggregate) {
        result = snapAggregate();
      }
    }
    return result;
  }

  private KlineTick append(KlineTick newObj) {
    int idx = calculateIdx(newObj.getTime());
    if (idx < 0 || idx >= numOfPeriod) {
      return null;
    }
    KlineTick oldObj = (KlineTick) data[idx];
    if (oldObj != null) {
      boolean isInSamePeriod = alignWithPeriod(newObj.getTime(), period) == alignWithPeriod(oldObj.getTime(), period);
      if (isInSamePeriod) {
        if (oldObj.getId().equals(newObj.getId())) {
          // same period
          // rollback Aggregate before merge
          doRollbackAggregate(oldObj);
          data[idx] = oldObj.merge(newObj);
        } else {
          data[idx] = oldObj.sum(newObj);
        }
      } else {
        data[idx] = newObj;
      }
    } else {
      data[idx] = newObj;
    }
    return (KlineTick) data[idx];
  }

  private boolean updateWindow() {
    long now = alignWithPeriod(System.currentTimeMillis(), period);
    int roteCount = Math.toIntExact((now - tt) / period);
    if (roteCount != 0) {
      clearAggregate();
      if (roteCount < numOfPeriod) {
        int sPos = roteCount;
        int dPos = 0;
        int length = numOfPeriod - roteCount;
        for (int i = 0; i < length; i++) {
          this.data[dPos] = this.data[sPos];
          // clear src data
          this.data[sPos] = null;
          doAggregate((KlineTick) this.data[dPos]);
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

  private void clearAggregate() {
    if (!autoAggregate)
      return;
    low = high = vol = open = close = amount = BigDecimal.ZERO;
    count = 0;
  }

  private void doAggregate(KlineTick tick) {
    if (!autoAggregate || tick == null) {
      return;
    }
    count += tick.getCount();
    amount = amount.add(tick.getAmount());
    vol = vol.add(tick.getVol());
    close = tick.getClose();
    if (open.compareTo(BigDecimal.ZERO) == 0) {
      open = tick.getOpen();
    }
    close = tick.getClose();
    if (tick.getHigh().compareTo(high) > 0) {
      high = tick.getHigh();
    }
    if (tick.getLow().compareTo(low) > 0) {
      low = tick.getLow();
    }
  }

  private void doRollbackAggregate(KlineTick oldTick) {
    if (!autoAggregate || oldTick == null) {
      return;
    }
    count -= oldTick.getCount();
    amount = amount.subtract(oldTick.getAmount());
    vol = vol.subtract(oldTick.getVol());
  }

  private MarketDetailTick snapAggregate() {
    if (!autoAggregate) {
      return null;
    }
    MarketDetailTick detail = new MarketDetailTick();
    detail.setVol(vol);
    detail.setAmount(amount);
    detail.setClose(close);
    detail.setOpen(open);
    detail.setCount(count);
    detail.setHigh(high);
    detail.setLow(low);
    return detail;
  }

  private int calculateIdx(long t) {
    long at = alignWithPeriod(t, period);
    return Math.toIntExact(((at - ht) % totalPeriodSize) / period);
  }

  private long alignWithPeriod(long t, long p) {
    return t - t % p;
  }
}
