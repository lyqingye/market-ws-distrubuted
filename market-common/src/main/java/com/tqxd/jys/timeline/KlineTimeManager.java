package com.tqxd.jys.timeline;

import com.tqxd.jys.constance.Period;
import com.tqxd.jys.messagebus.payload.detail.MarketDetailTick;
import com.tqxd.jys.timeline.cmd.CmdResult;
import com.tqxd.jys.utils.VertxUtil;
import io.vertx.core.Vertx;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

public class KlineTimeManager {
    /**
     * 名称 -> timeLine 映射
     */
    private Map<String, KlineTimeLine> timeLineMap = new ConcurrentHashMap<>();

    /**
     * 聚合数据消费者
     * String -> k线名称
     * MarketDetailTick -> 聚合数据
     */
    private BiConsumer<String, MarketDetailTick> aggregateConsumer;

    /**
     * vertx
     */
    private Vertx vertx;

    private KlineTimeManager() {
    }

    public static KlineTimeManager create(Vertx vertx, BiConsumer<String, MarketDetailTick> aggregateConsumer) {
        KlineTimeManager mgr = new KlineTimeManager();
        mgr.vertx = Objects.requireNonNull(vertx);
        mgr.aggregateConsumer = Objects.requireNonNull(aggregateConsumer);
        mgr.startTickKlineThread();
        return mgr;
    }

    public KlineTimeLine getOrCreate(String name, Period period) {
        return timeLineMap.computeIfAbsent(name, k -> new KlineTimeLine(name, period.getMill(), period.getNumOfPeriod(), period.equals(Period._1_MIN)));
    }

    private void startTickKlineThread() {
        Thread thread = new Thread(tickJob());
        thread.setName("[Kline-TimeLine-Tick]");
        thread.setDaemon(false);
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
            // tick 所有k线
            for (KlineTimeLine timeLine : timeLineMap.values()) {
                if (timeLine != null) {
                    // tick当前k线
                    CmdResult<MarketDetailTick> result = timeLine.tick();
                    if (result.isSuccess()) {
                        try {
                            // k线窗口滑动，触发了数据聚合
                            MarketDetailTick aggregate = result.get();
                            if (aggregate != null) {
                                // 异步消费聚合数据
                                VertxUtil.asyncFastCallIgnoreRs(vertx, () -> {
                                    aggregateConsumer.accept(timeLine.name(), aggregate);
                                });
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
    }
}
