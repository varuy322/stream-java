package com.sdu.spark.structstraming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * todo: metadataPath配置
  *
  * @author hanhan.zhang
  * */
object WordCountStatistics {

  def main(args: Array[String]): Unit = {

//    val sparkSession = SparkSession.builder()
//                                   .appName("word_count_statistics")
//                                   .getOrCreate()
//    // SparkSession隐式定义
//    import sparkSession.implicits._
//
//    // 读取数据源
//    val lines = sparkSession.readStream
//                            .format("socket")
//                            .option("host", "localhost")
//                            .option("port", 6712)
//                            .option("includeTimestamp", true)
//                            .load()
//
//    // 数据转换
//    val wordTable = lines.as[(String, Timestamp)]
//                         .flatMap(line => line._1.split(" ").map(word => (word, line._2)))
//                         .toDF("word", "timestamp")
//
//    // 统计窗口内数据
//    val windowResult = wordTable.groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
//                                .count()
//                                .orderBy("window")
//
//    // 数据查询
//    val query = windowResult.writeStream
//                            .queryName("WordStatisticQuery")
//                            .outputMode("complete")
//                            .format("console")
//                            .option("truncate", "false")
//                            .start()
//
//    query.awaitTermination()
  }

}
