package com.tqxd.jys.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * 基于 Disruptor 的队列
 *
 * @author lyqingye
 * @since 2020/9/24 下午3:31
 */
public class DisruptorQueue<T> {
    private Disruptor<ObjectEvent<T>> disruptor;
    private RingBuffer<ObjectEvent<T>> ringBuffer;

    public DisruptorQueue(Disruptor<ObjectEvent<T>> disruptor) {
        this.disruptor = disruptor;
        this.ringBuffer = disruptor.getRingBuffer();
        this.disruptor.start();
    }

    /**
     * 入队操作
     *
     * @param t 对象
     */
    public void add(T t) {
        long sequence = this.ringBuffer.next();
        ObjectEvent<T> event = this.ringBuffer.get(sequence);
        event.setObj(t);
        this.ringBuffer.publish(sequence);
    }

    /**
     * 是否为空
     */
    public boolean isEmpty() {
        return this.ringBuffer.remainingCapacity() == this.ringBuffer.getBufferSize();
    }

    /**
     * 当前元素数量
     */
    public long size() {
        return this.ringBuffer.getBufferSize() - this.ringBuffer.remainingCapacity();
    }

    /**
     * 销毁队列
     */
    public void shutdown() {
        disruptor.shutdown();
    }
}
