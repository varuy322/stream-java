package com.sdu.storm.topology.window;

import com.google.common.collect.Maps;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class WordCountBolt extends BasicWindowBolt<Tuple> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(List<Tuple> windowTuples, TimeWindow window) {
        if (windowTuples == null || windowTuples.isEmpty()) {
            return;
        }

        Map<String, Integer> counter = Maps.newHashMap();
        for (Tuple tuple : windowTuples) {
            String word = tuple.getString(0);
            long timestamp = tuple.getLong(1);
            LOGGER.info("TimeWindow: {}, word: {}, timestamp: {}", window, word, timestamp);

            int num = counter.getOrDefault(word, 0);
            num += 1;
            counter.put(word, num);
        }

        LOGGER.info("TimeWindow: {}, Word Statistic: {}", window, mapToString(counter));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private static String mapToString(Map<String, Integer> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue());
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
        }

        sb.append("]");

        return sb.toString();
    }
}
