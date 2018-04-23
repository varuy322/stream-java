package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.schema.RTDDomainSource;

public class DataParserFactory {

    public static JSONDataParser createJsonDataParser(String topic, RTDDomainSource domainSource) {
        return JSONDataParser.of(topic, domainSource);
    }

}
