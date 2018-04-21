package com.sdu.stream.storm.node.bolt;

import com.google.common.collect.Lists;
import com.sdu.stream.storm.parse.DataParser;
import com.sdu.stream.storm.parse.DataParserFactory;
import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.schema.RTDConf;
import com.sdu.stream.storm.utils.JsonUtils;
import com.sdu.stream.storm.utils.RTDParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.sdu.stream.storm.utils.JsonUtils.toJson;

/**
 * 订阅"Topic"数据流并标准化
 *
 * @author hanhan.zhang
 * */
public class RTDStandardBolt extends RTDBaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(RTDStandardBolt.class);

    private OutputCollector collector;

    private transient int version;

    private String topic;

    private transient DataParser<String> dataParser;

    public RTDStandardBolt(String topic) {
        this.topic = topic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        RTDConf conf = checkAndGetRTDConf(stormConf);
        if (conf == null || conf.getTopicJsonSchemas() == null || conf.getTopicJsonSchemas().isEmpty()) {
            throw new IllegalArgumentException("Topology Topic Parse Schema Empty !!!");
        }

        if (conf.getTopicJsonSchemas().get(topic) == null) {
            throw new IllegalArgumentException("Topology Topic Parse Schema Empty, Topic: " + topic);
        }

        this.version = conf.getVersion();
        this.dataParser = DataParserFactory.createDataParser(topic, conf.getTopicJsonSchemas().get(topic));
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
        RTDConf conf = JsonUtils.fromJson(schemaJson, RTDConf.class);
        if (conf != null && conf.getTopicJsonSchemas() != null && conf.getTopicJsonSchemas().get(topic) != null) {
            this.dataParser = DataParserFactory.createDataParser(topic, conf.getTopicJsonSchemas().get(topic));
        }
    }

    @Override
    public void executeBySchema(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(topic)) {
            String json = tuple.getStringByField("record");
            if (isNullOrEmpty(json)) {
                // TODO: 监控
                collector.ack(tuple);
                return;
            }

            // 数据流标准化
            try {
                DataRow newTuple = dataParser.parse(json);
                collector.emit(topic, tuple, Lists.newArrayList(topic, toJson(newTuple)));
                collector.ack(tuple);
            } catch (RTDParseException e) {
                // TODO: 监控
                LOGGER.error("RTD standard data failure, origin data: {}", json, e);
                collector.ack(tuple);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(topic, new Fields("topic", "dataRow"));
    }
}
