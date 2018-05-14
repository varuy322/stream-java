package com.sdu.storm.topology.window;

import com.google.common.collect.Lists;

import java.util.Collection;

/**
 * 滚动窗口
 *
 * @author hanhan.zhang
 * */
public class TumblingCountWindows<T> extends WindowAssigner<T> {

    // 滚动窗口长度
    private final long size;

    // 标识已接收元素序号(全局)
    private long offset = -1;

    private TumblingCountWindows(long size) {
        this.size = size;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        offset++;

        long start = offset / size * size;
        return Lists.newArrayList(new TimeWindow(start, start + size));
    }

    @Override
    public String toString() {
        return "TumblingCountWindows";
    }

    public static <T> TumblingCountWindows<T> create(long size) {
        return new TumblingCountWindows<>(size);
    }

    public static void main(String[] args) {
        // o o o o o o o o o o
        // 0 1 2 3 4 5 6 7 8 9
        TumblingCountWindows<Integer> assigner = TumblingCountWindows.create(3);
        for (int i = 0; i < 10; ++i) {
            System.out.println("第" + (i + 1) + "个Tuple属于窗口:");
            Collection<TimeWindow> windows = assigner.assignWindows(i, 0);
            for (TimeWindow window : windows) {
                System.out.println(window);
            }
        }
    }
}
