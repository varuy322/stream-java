package com.sdu.storm.topology.window;

import java.io.Serializable;
import java.util.concurrent.ScheduledFuture;

public abstract class Trigger<T> implements Serializable{

    /**
     * Bolt接收Tuple数据时, 是否触发窗口操作
     *
     * @param window: element所在窗口
     * */
    public abstract TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx);

    public interface TriggerContext {

        /**允许的最大延迟时间*/
        long getMaxLagMs();

        ScheduledFuture<?> deleteEventTimeTimer(TimeWindow window);

        /**
         * @param time: 窗口清除截止时间戳
         * @param window: Tuple所属窗口
         * */
        void registerEventTimeTimer(long time, TimeWindow window);

    }
}
