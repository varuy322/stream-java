package com.sdu.stream.storm.node;

import com.sdu.stream.storm.utils.RTDConf;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static com.sdu.stream.storm.utils.JsonUtils.fromJson;
import static com.sdu.stream.storm.utils.RTDConf.RTD_STANDARD_OUTPUT_FIELDS;
import static com.sdu.stream.storm.utils.RTDConf.RTD_STANDARD_STREAM;

/**
 * 实时数据标准化节点
 *
 * @author hanhan.zhang
 * */
public class RTDStandardExecuteBolt extends BaseRichBolt  {

    private OutputCollector _collector;
    private RTDConf conf;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        conf = fromJson((String) stormConf.get(RTD_STANDARD_STREAM), RTDConf.class);
    }

    @Override
    public void execute(Tuple input) {
        String topic = input.getStringByField("topic");
        String record = input.getStringByField("record");

        // 根据JsonPath配置提取数据
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RTD_STANDARD_STREAM,  false, RTD_STANDARD_OUTPUT_FIELDS);
    }
}
