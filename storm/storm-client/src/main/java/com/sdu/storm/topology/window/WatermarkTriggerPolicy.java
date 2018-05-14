package com.sdu.storm.topology.window;

/**
 *
 * */
public enum WatermarkTriggerPolicy {

    /**
     * global max timestamp, window will be fired as long as a watermark from one task > window end
     */
    GLOBAL_MAX_TIMESTAMP,

    /**
     * max timestamp + received watermark task ratio
     */
    MAX_TIMESTAMP_WITH_RATIO,

    /**
     * received watermarks from all upstream tasks and all watermarks > window end
     */
    TASK_MAX_GLOBAL_MIN_TIMESTAMP

}
