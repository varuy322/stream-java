package com.sdu.spark.streaming.socket

import com.sdu.spark.{Message, Utils}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * @author hanhan.zhang
  * */
object SocketWordStatistic {

  def main(args: Array[String]): Unit = {
    // Spark配置参数
    val conf = new SparkConf()
    conf.setAppName("socket_word_statistic")
    conf.set("spark.streaming.fetcher.host", Utils.getLocalIp)
    conf.set("spark.streaming.fetcher.port", "6719")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    // 创建Socket数据流
    val socketStream = SocketUtils.createStream[Message](ssc, StorageLevel.MEMORY_ONLY)

    // 数据统计
    socketStream.map(msg => (msg.word, 1))
                .reduceByKey((x1, x2) => x1 + x2)
                .print()

    // 启动程序
    ssc.start()
    ssc.awaitTermination()
  }

}
