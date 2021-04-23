package com.tqxd.jys.timeline;

import com.tqxd.jys.common.payload.KlineTick;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.timeline.cmd.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.tqxd.jys.utils.TimeUtils.alignWithPeriod;

public class KlineTimeLine {
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
   * 命令队列
   */
  private ConcurrentLinkedQueue<Object> cmdBuffer = new ConcurrentLinkedQueue<Object>();
  /**
   *
   */
  private KlineTimeLineMeta meta;

  public KlineTimeLine(KlineTimeLineMeta meta, long period, int numOfPeriod, boolean autoAggregate) {
    this.meta = meta;
    this.period = period;
    this.numOfPeriod = numOfPeriod;
    this.totalPeriodSize = period * numOfPeriod;
    long now = System.currentTimeMillis();
    this.tt = alignWithPeriod(now, period);
    this.data = new Object[numOfPeriod];
    this.ht = tt - totalPeriodSize + period;
    this.autoAggregate = autoAggregate;
  }

  public KlineTimeLineMeta meta() {
    return this.meta;
  }

  public CmdResult<List<KlineTick>> poll(long start, long end, int partIdx) {
    PollTicksCmd cmd = new PollTicksCmd();
    cmd.setEndTime(end);
    cmd.setStartTime(start);
    cmd.setPartIdx(partIdx);
    cmdBuffer.offer(cmd);
    return cmd.getResult();
  }

  public CmdResult<UpdateTickResult> update(long commitIndex, KlineTick tick) {
    UpdateTickCmd cmd = new UpdateTickCmd();
    cmd.setTick(tick);
    cmd.setCommitIndex(commitIndex);
    cmdBuffer.offer(cmd);
    return cmd.getResult();
  }

  public CmdResult<MarketDetailTick> applySnapshot(long commitIndex, List<KlineTick> ticks) {
    ApplySnapshotCmd cmd = new ApplySnapshotCmd();
    cmd.setCommitIndex(commitIndex);
    cmd.setTicks(ticks);
    cmdBuffer.offer(cmd);
    return cmd.getResult();
  }

  public MarketDetailTick tick() {
    MarketDetailTick result = null;
    if (execUpdateWindow()) {
      if (autoAggregate) {
        result = snapAggregate();
      }
    }
    Object cmd;
    while ((cmd = cmdBuffer.poll()) != null) {
      if (cmd instanceof UpdateTickCmd) {
        execUpdateTick((UpdateTickCmd) cmd);
      } else if (cmd instanceof PollTicksCmd) {
        execPollTicks((PollTicksCmd) cmd);
      } else if (cmd instanceof ApplySnapshotCmd) {
        execApplySnapshot((ApplySnapshotCmd) cmd);
      }
    }
    return result;
  }

  private void execUpdateTick(UpdateTickCmd cmd) {
    if (cmd.getCommitIndex() <= meta.getCommitIndex()) {
      cmd.getResult().setSuccess(false);
      cmd.getResult().setReason("invalid commit index cur commitIndex: " + meta.getCommitIndex() + " cmd commitIndex: " + cmd.getCommitIndex());
      cmd.getResult().complete(null);

      return;
    }
    KlineTick newObj = cmd.getTick();
    KlineTick updateTick = applyTick(newObj);
    if (updateTick == null) {
      cmd.getResult().setSuccess(false);
      cmd.getResult().setReason("apply tick fail! index outbound: " + calculateIdx(newObj.getTime()));
      cmd.getResult().complete(null);
      return;
    }
    // aggregate the window
    doAggregate(newObj);
    // complete
    cmd.getResult().setSuccess(true);
    cmd.getResult().complete(new UpdateTickResult(this.meta, updateTick, snapAggregate()));
    meta.applyCommitIndex(cmd.getCommitIndex());
  }

  private KlineTick applyTick(KlineTick newObj) {
    int idx = calculateIdx(newObj.getTime());
    if (idx < 0 || idx >= numOfPeriod) {
      return null;
    }
    KlineTick oldObj = (KlineTick) data[idx];
    if (oldObj != null) {
      if (alignWithPeriod(newObj.getTime(), period) != alignWithPeriod(oldObj.getTime(), period)) {
        data[idx] = newObj;
      } else {
        data[idx] = oldObj.merge(newObj);
      }
    } else {
      data[idx] = newObj;
    }
    return (KlineTick) data[idx];
  }

  private void execPollTicks(PollTicksCmd cmd) {
    int partIdx = cmd.getPartIdx();
    long startTime = cmd.getStartTime();
    long endTime = cmd.getEndTime();
    final int partSize = 300;
    int startIdx = partSize * partIdx;
    int endIdx = Math.min(startIdx + partSize, numOfPeriod);
    List<KlineTick> result = new ArrayList<>(partSize);
    while (startIdx < endIdx) {
      KlineTick obj = (KlineTick) this.data[startIdx];
      if (obj != null) {
        if (obj.getTime() >= startTime && obj.getTime() <= endTime) {
          result.add(obj);
        }
      }
      startIdx++;
    }
    cmd.getResult().setSuccess(true);
    cmd.getResult().complete(result);
  }

  private boolean execUpdateWindow() {
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

  private void execApplySnapshot(ApplySnapshotCmd cmd) {
    long commitIndex = cmd.getCommitIndex();
    if (commitIndex < 0) {
      cmd.getResult().setSuccess(false);
      cmd.getResult().setReason("invalid commit index while apply the snapshot! commit index: " + cmd.getCommitIndex());
      return;
    }
    meta.applyCommitIndex(commitIndex);
    for (KlineTick tick : cmd.getTicks()) {
      KlineTick newObj = applyTick(tick);
      if (newObj != null) {
        doAggregate(newObj);
      }
    }
    cmd.getResult().setSuccess(true);
    cmd.getResult().complete(snapAggregate());
  }

  private void clearAggregate() {
    if (!autoAggregate)
      return;
    low = high = vol = open = close = amount = BigDecimal.ZERO;
    count = 0;
  }

  private void doAggregate(KlineTick tick) {
    if (!autoAggregate || tick == null)
      return;
    count += tick.getCount();
    amount = amount.add(tick.getAmount());
    vol = vol.add(tick.getVol());
    close = tick.getClose();
    if (open.compareTo(BigDecimal.ZERO) == 0)
      open = tick.getOpen();
    close = tick.getClose();
    if (tick.getHigh().compareTo(high) > 0)
      high = tick.getHigh();
    if (tick.getLow().compareTo(low) > 0)
      low = tick.getLow();
  }

  private MarketDetailTick snapAggregate() {
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

  private String dumpData() {
    StringBuilder sb = new StringBuilder();
    for (Object obj : data) {
      KlineTick tick = KlineTick.class.cast(obj);
      if (tick != null) {
        sb.append(tick.getTime());
      } else {
        sb.append("null");
      }
      sb.append(" | ");
    }
    return sb.toString();
  }
}
