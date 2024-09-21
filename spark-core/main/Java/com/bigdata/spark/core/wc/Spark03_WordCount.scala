package com.bigdata.spark.core.wc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    // Log
    Logger.getLogger("org").setLevel(Level.WARN)

    // Application
    // Spark 框架
    // TODO 建立和 Spark 框架的連接
    // JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 執行業務操作

    val lines: RDD[String] = sc.textFile(path = "data")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    // Spark 框架提供了更多的功能，可以將分組和聚合使用一個方法實現
    // reduceByKey: 相同的 Key 的數據，可以對 value 進行 reduce 聚合
    val wordToCount = wordToOne.reduceByKey(_ + _)

    val array = wordToCount.collect()
    array.foreach(println)

    // TODO 關閉連接
    sc.stop()
  }
}
