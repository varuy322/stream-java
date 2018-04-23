package com.sdu.stream.storm.node.bolt;

import com.google.common.collect.Maps;
import com.sdu.stream.storm.parse.DataParserFactory;
import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.parse.JSONDataParser;
import com.sdu.stream.storm.schema.RTDDomainSchema;
import com.sdu.stream.storm.schema.RTDDomainSource;
import com.sdu.stream.storm.utils.JsonUtils;
import com.sdu.stream.storm.utils.RTDParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.sdu.stream.storm.node.bolt.IRTDSchemaBolt.RTDActionType.StandardType;
import static com.sdu.stream.storm.utils.JsonUtils.toJson;
import static org.apache.storm.shade.com.google.common.collect.Lists.newArrayList;

/**
 * 订阅"Topic"数据流并标准化
 *
 * @author hanhan.zhang
 * */
public class RTDStandardBolt extends RTDBaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(RTDStandardBolt.class);

    private OutputCollector collector;

    private Set<String> topics;

    private transient int version;
    private transient Map<String, JSONDataParser> dataParsers;

    public RTDStandardBolt(Set<String> topics) {
        this.topics = topics;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        RTDDomainSchema schema = checkAndGetRTDConf(stormConf);
        if (schema == null || schema.getSource() == null || schema.getSource().isEmpty()) {
            throw new IllegalArgumentException("RTD standard data source empty !!!");
        }

        this.version = schema.getVersion();
        this.dataParsers = updateStandardScheme(schema);

        this.collector = collector;
    }

    @Override
    public void schemaUpdate(int version, String schemaJson) {
        if (version <= this.version) {
            LOGGER.debug("RTD schema version already out of data, current version: {}", this.version);
            return;
        }
        if (schemaJson == null || schemaJson.isEmpty()) {
            // TODO: 报警
            LOGGER.debug("Topology RTD conf empty !!!");
            return;
        }
        RTDDomainSchema schema = JsonUtils.fromJson(schemaJson, RTDDomainSchema.class);
        if (schema == null || schema.getSource() == null || schema.getSource().isEmpty()) {
            // TODO: 报警
            return;
        }
        this.version = version;
        this.dataParsers = updateStandardScheme(schema);
    }

    @Override
    public void executeBySchema(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        JSONDataParser dataParser = this.dataParsers.get(streamId);
        if (dataParser == null) {
            LOGGER.warn("Standard bolt receive unknown topic: {}", streamId);
            return;
        }
        try {
            DataRow dataRow = dataParser.parse(tuple.getStringByField("record"));
            collector.emit(streamId, tuple, newArrayList(streamId, StandardType, toJson(dataRow)));
            collector.ack(tuple);
        } catch (RTDParseException e) {
            // TODO: 报警
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        assert topics != null && topics.size() > 0;
        for (String topic : topics) {
            declarer.declareStream(topic, new Fields(RTD_ACTION_TOPIC, RTD_ACTION_TYPE, RTD_ACTION_RESULT));
        }
    }

    private Map<String, JSONDataParser> updateStandardScheme(RTDDomainSchema schema) {
        Map<String, JSONDataParser> dataParsers = Maps.newHashMap();
        List<RTDDomainSource> domainSources = schema.getSource();
        for (RTDDomainSource source : domainSources) {
            if (!topics.contains(source.getTopic())) {
                continue;
            }
            JSONDataParser dataParser = DataParserFactory.createJsonDataParser(source.getTopic(), source);
            dataParsers.put(source.getTopic(), dataParser);
        }
        return dataParsers;
    }
}
