package com.sdu.storm.topology.window;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

public class SlidingEventTimeWindows<T> extends WindowAssigner<T> {

    private final long size;
    private final long slide;

    private SlidingEventTimeWindows(long size, long slide) {
        this.size = size;
        this.slide = slide;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        // 分析同SlidingCountWindows相反, 计算最后一个窗口起始位置
        long lastStart = timestamp - timestamp % slide;

        List<TimeWindow> windows = Lists.newLinkedList();
        for (long start = lastStart; start + size > timestamp; start -= slide) {
            windows.add(new TimeWindow(start, start + size));
        }

        return windows;
    }

    public static <T> SlidingEventTimeWindows<T> create(long size, long slide) {
        return new SlidingEventTimeWindows<>(size, slide);
    }
}
