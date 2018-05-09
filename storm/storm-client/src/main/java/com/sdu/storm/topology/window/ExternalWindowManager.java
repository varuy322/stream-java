package com.sdu.storm.topology.window;

import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TriggerHandler;
import org.apache.storm.windowing.TriggerPolicy;

/**
 * {@link ExternalWindowManager}处理两类Event:
 *
 * 1: Tuple Event(EventImpl)
 *
 * 2: Watermark Event
 *
 * @author hanhan.zhang
 * */
public interface ExternalWindowManager<T> extends TriggerHandler {

    default void add(T event) {
        add(event, System.currentTimeMillis());
    }

    default void add(T event, long ts) {
        add(new EventImpl<>(event, ts));
    }

    void add(Event<T> event);

    // 窗口输入数据流驱除策略
    void setEvictionPolicy(EvictionPolicy<T> evictionPolicy);
    // 窗口生成策略
    void setTriggerPolicy(TriggerPolicy<T> triggerPolicy);

    void shutdown();

}
