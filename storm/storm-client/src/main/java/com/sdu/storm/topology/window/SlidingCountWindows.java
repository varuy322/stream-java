package com.sdu.storm.topology.window;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * 滑动计数窗口
 *
 * @author hanhan.zhang
 * */
public class SlidingCountWindows<T> extends WindowAssigner<T> {

    // 滑动窗口长度
    private final long count;
    // 滑动窗口间隔(即每隔X个元素构建一个新窗口)
    private final long slide;

    // 标识已接收元素序号(全局)
    private long offset = -1;

    private SlidingCountWindows(long count, long slide) {
        this.count = count;
        this.slide = slide;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        offset++;

        // 分析: 滑动计数窗口中元素X可属于多个窗口，则需要计算元素X所归属的第1个窗口的起始位置
        //      计算元素X的第一个窗口起始位置, 按照滑动间隔计算
        long start = offset / slide * slide - slide;
        if (start < 0) {
            start = 0;
        }


        List<TimeWindow> timeWindows = Lists.newLinkedList();
        for (long nextStart = start; nextStart <= offset; nextStart += slide) {
            if (offset < nextStart + count) {
                timeWindows.add(new TimeWindow(nextStart, nextStart + count));
            }
        }

        return timeWindows;
    }


    public static <T> SlidingCountWindows<T> create(long count, long slide) {
        return new SlidingCountWindows<>(count, slide);
    }

    public static void main(String[] args) {
        // o o o o o o o o o o
        // 0 1 2 3 4 5 6 7 8 9
        SlidingCountWindows<Integer> windowAssigner = SlidingCountWindows.create(5, 3);
        for (int i = 0; i < 10; ++i) {
            System.out.println(String.format("第%d个元素所属窗口: ", i + 1));
            Collection<TimeWindow> windows = windowAssigner.assignWindows(1, 10);
            for (TimeWindow window : windows) {
                System.out.println(window);
            }
        }
    }
}
