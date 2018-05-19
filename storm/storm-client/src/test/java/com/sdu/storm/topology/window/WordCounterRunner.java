package com.sdu.storm.topology.window;

import com.google.common.collect.Maps;
import com.sdu.storm.topology.RTDTopologyBuilder;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.tuple.Fields;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.sdu.storm.configuration.ConfigConstants.TOPOLOGY_STATE_ROCKSDB_LIB_DIR;
import static com.sdu.storm.topology.utils.StormUtils.STORM_STATE_ROCKSDB_STORE_DIR;

public class WordCounterRunner {

    public static void main(String[] args) {
        Map<String, Object> stormConf = Maps.newHashMap();

        // RocksDB设置
        stormConf.put(TOPOLOGY_STATE_ROCKSDB_LIB_DIR, "/Users/hanhan.zhang/tmp/rocksdb-lib");
        stormConf.put(STORM_STATE_ROCKSDB_STORE_DIR, "file:/Users/hanhan.zhang/tmp/rocksdb-store");

        RTDTopologyBuilder topologyBuilder = new RTDTopologyBuilder();

        // Spout
        topologyBuilder.setSpout("DataSpout", new FastRandomSentenceSpout(), 1);

        // Bolt
        topologyBuilder.setBolt("SplitBolt", new SplitSentence(), 1)
                       .shuffleGrouping("DataSpout");

        WordCountBolt wordCountBolt = new WordCountBolt();
        wordCountBolt.eventTimeWindow(Time.of(1, TimeUnit.SECONDS))
                     .withTimestampExtractor(tuple -> tuple.getLong(1));
//                     .withListStateDescriptor(new TupleSerializer());

        topologyBuilder.setBolt("WordCounter", wordCountBolt, 1)
                       .fieldsGrouping("SplitBolt", new Fields("word"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(
                "WordCounterTopology",
                stormConf,
                topologyBuilder.createTopology());
    }

}
