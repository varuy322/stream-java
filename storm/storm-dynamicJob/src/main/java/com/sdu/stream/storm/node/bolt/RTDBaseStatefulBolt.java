package com.sdu.stream.storm.node.bolt;

import org.apache.storm.state.State;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;

import static com.sdu.stream.storm.node.spout.RTDSchemaCheckSpout.*;

/**
 * Bolt should record self schema message
 *
 * @author hanhan.zhang
 * */
public abstract class RTDBaseStatefulBolt<T extends State> extends BaseStatefulBolt<T> implements IRTDSchemaBolt {

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();
        if (isSchemaCheckStream(streamId)) {
            int version = input.getIntegerByField(RTD_SCHEMA_VERSION);
            String schemaJson = input.getStringByField(RTD_SCHEMA_CONTENT);
            schemaUpdate(version, schemaJson);
        } else {
            executeBySchema(input);
        }
    }

}
