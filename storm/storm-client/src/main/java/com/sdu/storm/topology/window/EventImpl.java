package com.sdu.storm.topology.window;

public class EventImpl<T> implements Event<T> {

    private final T event;
    private final long ts;

    public EventImpl(T event, long ts) {
        this.ts = ts;
        this.event = event;
    }

    @Override
    public long getTimestamp() {
        return ts;
    }

    @Override
    public T get() {
        return event;
    }

    @Override
    public boolean isWatermark() {
        return false;
    }

    @Override
    public String toString() {
        return "EventImpl{" +
                "event=" + event +
                ", ts=" + ts +
                '}';
    }
}
