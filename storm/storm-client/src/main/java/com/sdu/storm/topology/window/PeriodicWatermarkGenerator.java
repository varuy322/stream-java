package com.sdu.storm.topology.window;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.task.TopologyContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class PeriodicWatermarkGenerator implements WatermarkGenerator {

    private long maxLagMs;
    private long watermarkInterval;
    private transient Set<GlobalStreamId> inputStreams;
    private transient ConcurrentMap<GlobalStreamId, Long> upstreamWatermarks;

    private PeriodicWatermarkGenerator(long maxLagMs, long watermarkInterval) {
        this.maxLagMs = maxLagMs;
        this.watermarkInterval = watermarkInterval;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.upstreamWatermarks = Maps.newConcurrentMap();
        this.inputStreams = Sets.newHashSet();

        for (GlobalStreamId streamId : context.getThisSources().keySet()) {
            if (!streamId.get_streamId().equals(CheckpointSpout.CHECKPOINT_STREAM_ID)) {
                inputStreams.add(streamId);
            }
        }
    }

    @Override
    public long getCurrentWatermark() {
        long ts = 0;
        // only if some data has arrived on each input stream
        if (upstreamWatermarks.size() >= inputStreams.size()) {
            ts = Long.MAX_VALUE;
            for (Map.Entry<GlobalStreamId, Long> entry : upstreamWatermarks.entrySet()) {
                ts = Math.min(ts, entry.getValue());
            }
        }
        return ts - maxLagMs;
    }

    @Override
    public void track(GlobalStreamId streamId, long timestamp) {
        long maxTimestamp = upstreamWatermarks.getOrDefault(streamId, Long.MIN_VALUE);
        if (timestamp > maxTimestamp) {
            upstreamWatermarks.put(streamId, timestamp - maxLagMs);
        }
    }

    @Override
    public long getWatermarkInterval() {
        return watermarkInterval;
    }

    public static PeriodicWatermarkGenerator of(long maxLagMs, long watermarkInterval) {
        return new PeriodicWatermarkGenerator(maxLagMs, watermarkInterval);
    }
}
