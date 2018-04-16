package com.sdu.stream.storm.node.bolt;

import org.apache.storm.tuple.Tuple;

public interface IRTDSchemaBolt {

    String RTD_SCHEMA_CONF = "RTDSchemaConf";

    void schemaUpdate(int version, String schemaJson);

    void executeRTD(Tuple tuple);

}

