package com.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.intToLiteral

object Spark02_WordCount {
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

    val lines: RDD[String] = sc.textFile(path = "datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1
    )

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) =>
            list.reduce(
                  (t1, t2) => {
                    (t1._1, t1._2 + t2._2)
                  }
            )
      }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 關閉連接
    sc.stop()
  }
}
