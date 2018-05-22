package com.sdu.storm.topology.window;

import com.sdu.storm.state.ListStateDescriptor;
import com.sdu.storm.state.typeutils.base.ListSerializer;
import com.sdu.storm.topology.types.WindowTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

import static java.util.Collections.emptyList;

public abstract class BasicWindowBolt implements IWindowBolt {

    public static final long DEFAULT_SLIDE = -1L;

    private WindowAssigner<Tuple> windowAssigner;

    private ListStateDescriptor<WindowTuple> stateDescriptor = new ListStateDescriptor<>(
            "WindowTupleState",
            new ListSerializer<>(WindowTuple.WindowTupleSerializer.INSTANCE),
            emptyList()
    );

    // watermark
    private WatermarkGenerator watermarkGenerator;
    private TimestampExtractor timestampExtractor;


    private long maxLagMs;
    private long size;
    private long slide;

    /**
     * 滚动计数窗口
     * */
    public BasicWindowBolt countWindow(long size) {
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
    public BasicWindowBolt countWindow(long size, long slide) {
        ensurePositiveTime(size, slide);
        ensureSizeGreaterThanSlide(size, slide);

        setSizeAndSlide(size, slide);

        this.windowAssigner = SlidingCountWindows.create(size, slide);
        return this;
    }

    /**
     * 滚动计时窗口
     * */
    public BasicWindowBolt eventTimeWindow(Time size) {
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
    public BasicWindowBolt eventTimeWindow(Time size, Time slide) {
        long s = size.toMilliseconds();
        long l = slide.toMilliseconds();
        ensurePositiveTime(s, l);
        ensureSizeGreaterThanSlide(s, l);

        setSizeAndSlide(s, l);
        this.windowAssigner = SlidingEventTimeWindows.create(s, l);
        return this;
    }

    //
    public BasicWindowBolt withTimestampExtractor(TimestampExtractor timestampExtractor) {
        this.timestampExtractor = timestampExtractor;
        return this;
    }

    /**仅支持EventTimeWindow*/
    public BasicWindowBolt withWatermarkGenerator(WatermarkGenerator watermarkGenerator) {
        this.watermarkGenerator = watermarkGenerator;
        return this;
    }

    public BasicWindowBolt withMaxLagMs(Time maxLag) {
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

    public long getSize() {
        return size;
    }

    public long getSlide() {
        return slide;
    }

    public long getMaxLagMs() {
        return maxLagMs;
    }

    public WindowAssigner<Tuple> getWindowAssigner() {
        return windowAssigner;
    }

    public ListStateDescriptor<WindowTuple> getStateDescriptor() {
        return stateDescriptor;
    }

    public WatermarkGenerator getWatermarkGenerator() {
        return watermarkGenerator;
    }

    public TimestampExtractor getTimestampExtractor() {
        return timestampExtractor;
    }
}
