package com.sdu.storm.topology.window;

public class CountTrigger<T> extends Trigger<T> {

    // 窗口触发阈值
    private final long count;
    // 当前窗口已接收到的数据元素个数
    private long received;

    private CountTrigger(long count) {
        this.count = count;
        this.received = 0;
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) {
        received++;
        if (received >= count) {
            received = 0;
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public String toString() {
        return "CountTrigger[count=" + count + ']';
    }

    public static <T> CountTrigger<T> of(long count) {
        return new CountTrigger<>(count);
    }
}
