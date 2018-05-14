package com.sdu.storm.topology.window;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.Map;

public interface WatermarkGenerator extends Serializable {

    void prepare(Map stormConf, TopologyContext context);

    /**当前水位线*/
    long getCurrentWatermark();

    void track(GlobalStreamId streamId, long timestamp);

    long getWatermarkInterval();

}
