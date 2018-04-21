package com.sdu.stream.storm.parse;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.sdu.stream.storm.schema.JSONSchema;
import com.sdu.stream.storm.utils.ColumnType;
import com.sdu.stream.storm.utils.RTDParseException;

import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

public class JSONDataParser implements DataParser<String> {

    private static final Configuration conf;

    static {
        conf = Configuration.defaultConfiguration();
    }

    private String topic;
    private JSONSchema schema;

    public JSONDataParser(String topic, JSONSchema schema) {
        this.topic = topic;
        this.schema = schema;
    }

    @Override
    public DataRow parse(String json) throws RTDParseException {
        if (isNullOrEmpty(json)) {
            throw new RTDParseException("Json empty !!!");
        }
        ReadContext ctx = JsonPath.using(conf).parse(json);

        String[] columnNames = new String[schema.schemaLength()];
        ColumnType[] columnTypes = new ColumnType[schema.schemaLength()];
        Object[] columnValues = new Object[schema.schemaLength()];

        Map<String, String> jsonConf = schema.schemaJSONConf();
        Map<String, ColumnType> schemaType = schema.schemaType();
        int index = 0;
        for (Map.Entry<String, String> entry : jsonConf.entrySet()) {
            String columnName = entry.getKey();
            String jsonPath = entry.getValue();
            ColumnType columnType = schemaType.get(columnName);
            Object columnValue = ctx.read(jsonPath, columnType.columnType());

            columnNames[index] = columnName;
            columnTypes[index] = columnType;
            columnValues[index] = columnValue;

            ++index;
        }

        return DataRow.of(topic, columnNames, columnTypes, columnValues);
    }

    public static void main(String[] args) {

    }
}
