package com.sdu.storm.topology.window;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class FastRandomSentenceSpout implements IRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;
    long startTime;
    long sentNum = 0;
    long maxSendNum;
    int index = 0;

    private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers",
            "JStorm is a distributed and fault-tolerant realtime computation system",
            "Inspired by Apache Storm, JStorm has been completely rewritten in Java and provides many more enhanced features",
            "JStorm has been widely used in many enterprise environments and proved robust and stable",
            "JStorm provides a distributed programming framework very similar to Hadoop MapReduce",
            "The developer only needs to compose his/her own pipe-lined computation logic by implementing the JStorm API",
            " which is fully compatible with Apache Storm API",
            "and submit the composed Topology to a working JStorm instance",
            "Similar to Hadoop MapReduce, JStorm computes on a DAG (directed acyclic graph).",
            "Different from Hadoop MapReduce a JStorm topology runs 24 * 7",
            "the very nature of its continuity abd 100% in-memory architecture",
            "has been proved a particularly suitable solution for streaming data and real-time computation",
            "JStorm guarantees fault-tolerance",
            "Whenever a worker process crashes",
            "the scheduler embedded in the JStorm instance immediately spawns a new worker process to take the place of the failed one",
            "The Acking framework provided by JStorm guarantees that every single piece of data will be processed at least once"};


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        startTime = System.currentTimeMillis();
        maxSendNum = 1000L;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if (sentNum >= maxSendNum) {
            sleepMs(1000);
            sentNum = 0;
            return;
        }

        sentNum++;
        String sentence = CHOICES[index++];
        if (index >= CHOICES.length) {
            index = 0;
        }
        _collector.emit(new Values(sentence));
        sleepMs(10);
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {
        _collector.emit(new Values(msgId), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
