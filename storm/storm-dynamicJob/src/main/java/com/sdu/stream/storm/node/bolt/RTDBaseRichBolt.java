package com.sdu.stream.storm.node.bolt;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import static com.sdu.stream.storm.node.spout.RTDSchemaCheckSpout.*;

public abstract class RTDBaseRichBolt extends BaseRichBolt implements IRTDSchemaBolt {

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();
        if (isSchemaCheckStream(streamId)) {
            String schemaJson = input.getStringByField(RTD_SCHEMA_CONTENT);
            int version = input.getIntegerByField(RTD_SCHEMA_VERSION);
            schemaUpdate(version, schemaJson);
        } else {
            executeBySchema(input);
        }
    }
}
