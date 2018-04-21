package com.sdu.stream.storm.node.bolt;

import com.sdu.stream.storm.schema.RTDConf;
import com.sdu.stream.storm.utils.JsonUtils;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

public interface IRTDSchemaBolt {

    String RTD_SCHEMA_CONF = "RTDSchemaConf";

    /**schema update with version*/
    void schemaUpdate(int version, String schemaJson);

    /**process tuple according schema*/
    void executeBySchema(Tuple tuple);

    default RTDConf checkAndGetRTDConf(Map stormConf) {
        String schemaJson = (String) stormConf.get(RTD_SCHEMA_CONF);
        if (isNullOrEmpty(schemaJson)) {
            throw new IllegalArgumentException("Topology RTDConf Empty !!!");
        }
        return JsonUtils.fromJson(schemaJson, RTDConf.class);
    }

}

