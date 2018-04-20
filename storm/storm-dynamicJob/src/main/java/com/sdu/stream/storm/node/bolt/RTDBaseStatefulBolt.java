package com.sdu.stream.storm.node.bolt;

import org.apache.storm.redis.state.RedisKeyValueState;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;

import static com.sdu.stream.storm.node.spout.RTDSchemaCheckSpout.RTD_SCHEMA_CONTENT;
import static com.sdu.stream.storm.node.spout.RTDSchemaCheckSpout.RTD_SCHEMA_VERSION;
import static com.sdu.stream.storm.node.spout.RTDSchemaCheckSpout.isSchemaCheckStream;

/**
 * Bolt should record self schema message
 *
 * @author hanhan.zhang
 * */
public abstract class RTDBaseStatefulBolt<K, V> extends BaseStatefulBolt<RedisKeyValueState<K, V>> implements IRTDSchemaBolt {

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
