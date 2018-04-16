package com.sdu.stream.storm.utils;

import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.state.RedisKeyValueStateProvider;

import static com.sdu.stream.storm.utils.JsonUtils.toJson;

/**
 * @author hanhan.zhang
 * */
public class JedisUtils {

    private static JedisPoolConfig redisPoolConf() {
        return new JedisPoolConfig("", 0, 100, "", 0);
    }

    private static JedisClusterConfig redisClusterConf() {
        return null;
    }

    public static String redisStateConf() {
        RedisKeyValueStateProvider.StateConfig conf = new RedisKeyValueStateProvider.StateConfig();
        conf.keyClass = String.class.getName();
        conf.valueClass = String.class.getName();
        conf.jedisPoolConfig = redisPoolConf();
        conf.jedisClusterConfig = redisClusterConf();

        return toJson(conf);
    }

}
