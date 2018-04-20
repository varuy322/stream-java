package com.sdu.stream.storm.node.bolt;

import org.apache.storm.tuple.Tuple;

public interface IRTDSchemaBolt {

    String RTD_SCHEMA_CONF = "RTDSchemaConf";

    /**schema update with version*/
    void schemaUpdate(int version, String schemaJson);

    /**process tuple according schema*/
    void executeBySchema(Tuple tuple);

}

