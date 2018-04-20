package com.sdu.stream.storm.node.bolt;

import com.sdu.stream.storm.schema.JSONSchema;
import com.sdu.stream.storm.schema.RTDConf;
import com.sdu.stream.storm.utils.JsonUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 *
 * @author hanhan.zhang
 * */
public class RTDStandardBolt extends RTDBaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(RTDStandardBolt.class);

    private OutputCollector collector;

    private transient int version;

    private String topic;

    private AtomicReference<JSONSchema> standardSchema = new AtomicReference<>();

    public RTDStandardBolt(String topic) {
        this.topic = topic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String schemaJson = (String) stormConf.get(RTD_SCHEMA_CONF);
        if (isNullOrEmpty(schemaJson)) {
            throw new IllegalArgumentException("Topology RTDConf Empty !!!");
        }
        RTDConf conf = JsonUtils.fromJson(schemaJson, RTDConf.class);
        if (conf == null || conf.getTopicJsonSchemas() == null || conf.getTopicJsonSchemas().isEmpty()) {
            throw new IllegalArgumentException("Topology Topic Parse Schema Empty !!!");
        }

        if (conf.getTopicJsonSchemas().get(topic) == null) {
            throw new IllegalArgumentException("Topology Topic Parse Schema Empty, Topic: " + topic);
        }

        this.version = conf.getVersion();
        this.standardSchema.set(conf.getTopicJsonSchemas().get(topic));
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
            this.standardSchema.set(conf.getTopicJsonSchemas().get(topic));
        }
    }

    @Override
    public void executeBySchema(Tuple tuple) {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("topic", new Fields("topic", "standard"));
    }
}
