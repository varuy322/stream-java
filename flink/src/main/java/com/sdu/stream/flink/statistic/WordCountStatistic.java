package com.sdu.stream.flink.statistic;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.flink.configuration.ConfigConstants.CHECKPOINTS_DIRECTORY_KEY;
import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION;


/**
 * @author hanhan.zhang
 * */
public class WordCountStatistic {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountStatistic.class);

    private static final String PATH = "file:///Users/hanhan.zhang/tmp/words.txt";
    private static final String EXTERNAL_CHECK_STATE_DIR = "file:///Users/hanhan.zhang/tmp/external/chk";
    private static final String CHECK_STATE_PATH = "file:///Users/hanhan.zhang/tmp/chk";

    // 是否只统计汉字
    private static final String STATISTIC_CHINESE_ONLY = "statistic.chinese.only";

    private static class TransferWord extends RichFlatMapFunction<String[], Tuple2<String, Integer>> {

        private static final String COUNT = "word.count";
        private boolean filter = false;
        private Pattern pattern;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 用户配置参数
            Map<String, String> conf = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
            filter = Boolean.parseBoolean(conf.getOrDefault(STATISTIC_CHINESE_ONLY, "false"));
            if (filter) {
                pattern = Pattern.compile("[\u4e00-\u9fa5]");
            }

            // Steam Accumulator
            getRuntimeContext().addAccumulator(COUNT, new IntCounter(0));
        }

        @Override
        public void flatMap(String[] value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String val : value) {
                getRuntimeContext().getAccumulator(COUNT).add(1);
                // 过滤非中文字
                if (filter && !pattern.matcher(val).find()) {
                    continue;
                }
                out.collect(new Tuple2<>(val, 1));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration cfg = new Configuration();
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(3, cfg);

        // 用户参数配置
        env.getConfig().setGlobalJobParameters(new GlobalJobParameters() {
            @Override
            public Map<String, String> toMap() {
                Map<String, String> conf = Maps.newHashMap();
                conf.put(STATISTIC_CHINESE_ONLY, "true");
                return conf;
            }
        });

        // Checkpoint配置
        CheckpointConfig conf = env.getCheckpointConfig();
        // 必须设置外存目录
        cfg.setString(CHECKPOINTS_DIRECTORY_KEY, EXTERNAL_CHECK_STATE_DIR);
        conf.enableExternalizedCheckpoints(DELETE_ON_CANCELLATION);
        conf.setCheckpointingMode(EXACTLY_ONCE);
        conf.setCheckpointInterval(5000L);
        conf.setCheckpointTimeout(5 * 60 * 1000L);

        // State管理
        env.setStateBackend(new RocksDBStateBackend(CHECK_STATE_PATH));

        DataStreamSource<String> dss = env.readTextFile(PATH);

        // StreamEnvironment构建Stream依赖及计算关系
        dss.filter(StringUtils::isNotEmpty).name("filter-stream").returns(String.class)
           .map(text -> StringUtils.split(text, " ")).name("split-stream").returns(String[].class)
           .flatMap(new TransferWord()).name("word-stream")
           .keyBy(tuple -> tuple.f0)
           .sum(1)
           .addSink(new PrintSinkFunction<>()).name("word-print");

        // StreamEnvironment
        JobExecutionResult execRes = env.execute("word-count-statistic");

        // Stream Accumulator统计信息
        Map<String, Object> accumulators = execRes.getAllAccumulatorResults();
        accumulators.forEach((name, accumulator) -> LOGGER.info("accumulator name: {}, result: {}", name, accumulator));
    }

}
