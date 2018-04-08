package com.sdu.stream.storm;


import com.sdu.stream.storm.utils.JedisUtils;
import com.sdu.stream.storm.utils.RTDConf;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.state.RedisKeyValueStateProvider;
import org.apache.storm.topology.TopologyBuilder;

import static org.apache.storm.Config.*;

/**
 * 实时数据数据收集作业
 *
 * @author hanhan.zhang
 * */
public class RTDCollectorJob {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        Config conf = new Config();

        // JsonPath配置
        conf.put(RTDConf.RTD_SCHEMA_CONF, "");

        // State Conf
        conf.put(TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 1000);
        conf.put(TOPOLOGY_MESSAGE_TIMEOUT_SECS, 2000);

        conf.put(TOPOLOGY_STATE_PROVIDER, RedisKeyValueStateProvider.class.getName());
        conf.put(TOPOLOGY_STATE_PROVIDER_CONFIG, JedisUtils.redisStateConf());

        String topologyName = "RTDCollectorJob";
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topologyName, conf, builder.createTopology());
    }


}
