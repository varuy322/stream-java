package com.sdu.stream.storm.node.bolt;

import com.sdu.stream.storm.utils.RTDType;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 *
 * @author hanhan.zhang
 * */
public class RTDStandardBolt extends RTDBaseRichBolt {

    private RTDType type;

    public RTDStandardBolt(RTDType type) {
        this.type = type;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String schemaJson = (String) stormConf.get(RTD_SCHEMA_CONF);
        if (isNullOrEmpty(schemaJson)) {
            throw new IllegalArgumentException("Topology execute schema conf empty !!!");
        }


    }

    @Override
    public void schemaUpdate(int version, String schemaJson) {

    }

    @Override
    public void executeRTD(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
