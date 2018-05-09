package com.sdu.storm.topology.window;

/**
 *
 * @author hanhan.zhang
 * */
interface Event<T> {

    long getTimestamp();

    T get();

    /**
     * 如果是WatermarkEvent, 则数据无需存储在外部(数据量较小且需要快速计算水位线)
     * */
    boolean isWatermark();

}
