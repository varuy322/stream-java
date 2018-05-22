package com.sdu.storm.topology.window;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitSentence implements IRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        long timestamp = System.currentTimeMillis();
        String sentence = tuple.getString(0);
        for (String word : sentence.split("\\s+")) {
            if (word == null || word.isEmpty() || word.equals(" ")) {
                continue;
            }
            collector.emit(new Values(word, timestamp));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
