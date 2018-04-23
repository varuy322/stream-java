package com.sdu.stream.storm.node.bolt;

import com.sdu.stream.storm.schema.RTDDomainSchema;
import com.sdu.stream.storm.utils.JsonUtils;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

public interface IRTDSchemaBolt {

    String RTD_ACTION_TOPIC = "RTDActionTopic";

    String RTD_ACTION_TYPE = "RTDActionType";

    String RTD_ACTION_RESULT = "RTDActionResult";

    String RTD_DOMAIN_SCHEMA = "RTDDomainSchema";

    /**schema update with version*/
    void schemaUpdate(int version, String schemaJson);

    /**process tuple according schema*/
    void executeBySchema(Tuple tuple);

    default RTDDomainSchema checkAndGetRTDConf(Map stormConf) {
        String schemaJson = (String) stormConf.get(RTD_DOMAIN_SCHEMA);
        if (isNullOrEmpty(schemaJson)) {
            throw new IllegalArgumentException("Topology RTDDomainSchema Empty !!!");
        }
        return JsonUtils.fromJson(schemaJson, RTDDomainSchema.class);
    }

    enum RTDActionType {
        StandardType, SumType
    }
}

