package com.sdu.storm.topology.window;

public class EventTimeTrigger<T> extends Trigger<T> {

    private EventTimeTrigger() {}

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) {
        ctx.registerEventTimeTimer(window.getEnd() + ctx.getMaxLagMs(), window);
        return TriggerResult.CONTINUE;
    }


    @Override
    public String toString() {
        return "EventTimeTrigger";
    }

    public static <T> EventTimeTrigger<T> create() {
        return new EventTimeTrigger<>();
    }
}
