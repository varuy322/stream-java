package com.sdu.storm.topology.window;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

public abstract class BasicWindowBolt<T extends Tuple> implements IWindowBolt<T> {

    public static final long DEFAULT_SLIDE = -1L;
    public static final long DEFAULT_STATE_SIZE = -1L;

    private WindowAssigner<T> windowAssigner;
    // 窗口状态(滚动窗口)
    private WindowAssigner<T> windowStateAssigner;

    // watermark
    private WatermarkGenerator watermarkGenerator;
    private WatermarkTriggerPolicy watermarkTriggerPolicy;
    private TimestampExtractor timestampExtractor;


    private long maxLagMs;
    private long size;
    private long slide;
    private long stateSize = DEFAULT_STATE_SIZE;

    /**
     * 滚动计数窗口
     * */
    public BasicWindowBolt<T> countWindow(long size) {
        ensurePositiveTime(size);

        setSizeAndSlide(size, DEFAULT_SLIDE);

        this.windowAssigner = TumblingCountWindows.create(size);
        return this;
    }

    /**
     * 滑动计数窗口
     * Note:
     *  窗口滑动长度要小于窗口长度
     * */
    public BasicWindowBolt<T> countWindow(long size, long slide) {
        ensurePositiveTime(size, slide);
        ensureSizeGreaterThanSlide(size, slide);

        setSizeAndSlide(size, slide);

        this.windowAssigner = SlidingCountWindows.create(size, slide);
        return this;
    }

    /**
     * 窗口状态(采用"滚动窗口")
     *
     * Note:
     *  状态窗口必须在窗口长度设置后再设置
     * */
    public BasicWindowBolt<T> withStateSize(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);
        ensureStateSizeGreaterThanWindowSize(this.size, s);

        this.stateSize = s;
        if (WindowAssigner.isEventTime(windowAssigner)) {
            this.windowStateAssigner = TumblingEventTimeWindow.create(s);
        }

        return this;
    }

    /**
     * 滚动计时窗口
     * */
    public BasicWindowBolt<T> eventTimeWindow(Time size) {
        long s = size.toMilliseconds();
        ensurePositiveTime(s);

        setSizeAndSlide(s, DEFAULT_SLIDE);
        this.windowAssigner = TumblingEventTimeWindow.create(s);
        return this;
    }

    /**
     * 滚动计时窗口
     * Note:
     *  窗口滑动时间长度要小于窗口时间长度
     * */
    public BasicWindowBolt<T> eventTimeWindow(Time size, Time slide) {
        long s = size.toMilliseconds();
        long l = slide.toMilliseconds();
        ensurePositiveTime(s, l);
        ensureSizeGreaterThanSlide(s, l);

        setSizeAndSlide(s, l);
        this.windowAssigner = SlidingEventTimeWindows.create(s, l);
        return this;
    }

    //
    public BasicWindowBolt<T> withTimestampExtractor(TimestampExtractor timestampExtractor) {
        this.timestampExtractor = timestampExtractor;
        return this;
    }

    /**仅支持EventTimeWindow*/
    public BasicWindowBolt<T> withWatermarkGenerator(WatermarkGenerator watermarkGenerator) {
        this.watermarkGenerator = watermarkGenerator;
        return this;
    }

    /**仅支持EventTimeWindow*/
    public BasicWindowBolt<T> withWatermarkTriggerPolicy(WatermarkTriggerPolicy triggerPolicy) {
        this.watermarkTriggerPolicy = triggerPolicy;
        return this;
    }

    public BasicWindowBolt<T> withMaxLagMs(Time maxLag) {
        this.maxLagMs = maxLag.toMilliseconds();
        ensureNonNegativeTime(maxLagMs);
        return this;
    }

    private void setSizeAndSlide(long size, long slide) {
        this.size = size;
        this.slide = slide;
    }

    private static void ensurePositiveTime(long... values) {
        for (long value : values) {
            if (value <= 0) {
                throw new IllegalArgumentException("time or slide must be positive!");
            }
        }
    }

    private static void ensureNonNegativeTime(long... values) {
        for (long value : values) {
            if (value < 0) {
                throw new IllegalArgumentException("time or slide must not be negative!");
            }
        }
    }

    private static void ensureSizeGreaterThanSlide(long size, long slide) {
        if (size <= slide) {
            throw new IllegalArgumentException("window size must be greater than window slide!");
        }
    }

    private static void ensureStateSizeGreaterThanWindowSize(long winSize, long stateSize) {
        if (winSize > stateSize) {
            throw new IllegalArgumentException("state window size must be greater than window size!");
        }
    }

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    public long getStateSize() {
        return stateSize;
    }

    public long getMaxLagMs() {
        return maxLagMs;
    }

    public WindowAssigner<T> getWindowAssigner() {
        return windowAssigner;
    }

    public WindowAssigner<T> getWindowStateAssigner() {
        return windowStateAssigner;
    }

    public WatermarkGenerator getWatermarkGenerator() {
        return watermarkGenerator;
    }

    public WatermarkTriggerPolicy getWatermarkTriggerPolicy() {
        return watermarkTriggerPolicy;
    }

    public TimestampExtractor getTimestampExtractor() {
        return timestampExtractor;
    }
}
