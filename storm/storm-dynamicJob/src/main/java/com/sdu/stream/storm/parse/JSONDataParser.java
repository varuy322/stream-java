package com.sdu.stream.storm.parse;

import com.google.common.collect.Maps;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.sdu.stream.storm.schema.RTDDomainSource;
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
    private RTDDomainSource source;

    private JSONDataParser(String topic, RTDDomainSource source) {
        this.topic = topic;
        this.source = source;
    }

    @Override
    public DataRow parse(String json) throws RTDParseException {
        if (isNullOrEmpty(json)) {
            throw new RTDParseException("Json empty !!!");
        }
        ReadContext ctx = JsonPath.using(conf).parse(json);

        Map<String, RTDDomainSource.JsonSchema> schemas = source.getStandard();
        if (schemas == null || schemas.isEmpty()) {
            throw new RTDParseException("Json schema empty !!!");
        }

        Map<String, DataRow.ColumnSchema> columns = Maps.newHashMap();
        for (Map.Entry<String, RTDDomainSource.JsonSchema> entry : schemas.entrySet()) {
            String columnName = entry.getKey();
            RTDDomainSource.JsonSchema schema = entry.getValue();
            ColumnType type = ColumnType.getColumnType(schema.getType());
            Object columnValue = ctx.read(schema.getPath(), type.columnType());

            DataRow.ColumnSchema columnSchema = new DataRow.ColumnSchema(columnName, type, columnValue);
            columns.put(columnName, columnSchema);
        }

        return DataRow.of(topic, columns);
    }

    public static JSONDataParser of(String topic, RTDDomainSource source) {
        return new JSONDataParser(topic, source);
    }

    public static void main(String[] args) {

    }
}
