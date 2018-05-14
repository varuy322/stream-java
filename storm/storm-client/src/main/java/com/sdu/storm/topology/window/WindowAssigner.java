package com.sdu.storm.topology.window;

import java.io.Serializable;
import java.util.Collection;

/**
 * {@link WindowAssigner}对Tuple数据划分窗口
 *
 * 1: 滑动窗口
 *
 *    Tuple数据可属于多个窗口
 *
 * 2: 滚动窗口
 *
 *    Tuple数据只属于一个窗口
 *
 * Note:
 *
 *  每个TimeWindow都有自己的Trigger(决定是否清楚窗口数据)
 *
 * @author hanhan.zhang
 * */
public abstract class WindowAssigner<T> implements Serializable {

    public abstract Collection<TimeWindow> assignWindows(T element, long timestamp);

    public static boolean isWindowCount(WindowAssigner windowAssigner) {
        return windowAssigner instanceof SlidingCountWindows ||
                windowAssigner instanceof TumblingCountWindows;
    }

    public static boolean isEventTime(WindowAssigner windowAssigner) {
        return windowAssigner instanceof SlidingEventTimeWindows ||
                windowAssigner instanceof TumblingEventTimeWindow;
    }
}
