package com.sdu.storm.topology.window;

public class WatermarkEvent<T> extends EventImpl<T> {

    public WatermarkEvent(long ts) {
        super(null, ts);
    }

    @Override
    public boolean isWatermark() {
        return true;
    }

    @Override
    public String toString() {
        return "WatermarkEvent[ts = " + getTimestamp() + "]";
    }
}
