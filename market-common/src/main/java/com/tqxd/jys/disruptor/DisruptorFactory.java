package com.tqxd.jys.disruptor;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author yjt
 * @since 2020/9/24 下午3:49
 */
public class DisruptorFactory {

  public static <T> DisruptorQueue<T> createQueue(int queueSize, AbstractDisruptorConsumer<T> consumer) {
    Disruptor<ObjectEvent<T>> disruptor = new Disruptor<>(new ObjectEventFactory<T>(), queueSize, Executors.defaultThreadFactory(), ProducerType.MULTI, new BlockingWaitStrategy());
    disruptor.handleEventsWith(consumer);
    return new DisruptorQueue<T>(disruptor);
  }

  public static <T> DisruptorQueue<T> createQueue(int queueSize, ThreadFactory threadFactory, AbstractDisruptorConsumer<T> consumer) {
    Disruptor<ObjectEvent<T>> disruptor = new Disruptor<>(new ObjectEventFactory<T>(), queueSize, threadFactory, ProducerType.MULTI, new BlockingWaitStrategy());
    disruptor.handleEventsWith(consumer);
    return new DisruptorQueue<T>(disruptor);
  }
}
