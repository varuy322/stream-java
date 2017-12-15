package com.sdu.stream.flink.statistic;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * @author hanhan.zhang
 * */
public class WindowWordsStatistic {

    private static final String HOST = "localhost";
    private static final int PORT = 6712;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(5, conf);

        DataStreamSource<String> dss = env.socketTextStream(HOST, PORT);

        dss.addSink(new PrintSinkFunction<>()).name("PrintSocketStream");

        // 启动作业
        env.execute("WindowWordsStatistic");
    }

}
