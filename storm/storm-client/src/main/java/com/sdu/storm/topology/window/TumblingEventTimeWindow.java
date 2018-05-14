package com.sdu.storm.topology.window;

import java.util.Collection;
import java.util.Collections;

/**
 * 滚动计时窗口
 *
 * @author hanhan.zhang
 * */
public class TumblingEventTimeWindow<T> extends WindowAssigner<T> {

    // 滚动窗口时间长度
    private long size;

    private TumblingEventTimeWindow(long size) {
        this.size = size;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        // 计算窗口起始位置
        long start = timestamp - timestamp % size;
        return Collections.singletonList(new TimeWindow(start, start + size));
    }

    public static <T> TumblingEventTimeWindow<T> create(long size) {
        return new TumblingEventTimeWindow<>(size);
    }
}
