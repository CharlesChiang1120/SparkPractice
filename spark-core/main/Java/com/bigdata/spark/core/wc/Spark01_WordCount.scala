package com.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark 框架
    // TODO 建立和 Spark 框架的連接
    // JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 執行業務操作

    // 1. 讀取文件，獲取一行一行的數據
    //    hello world
    val lines: RDD[String] = sc.textFile(path = "data")

    // 2. 將一行數據進行拆分，形成一個一個的單詞(分詞)
    //    扁平化: 將整體拆分成個體的操作
    //    "hello world" => hello, world, hello ,world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. 將數據根據單詞進行分組，便於統計
    //    (hello, hello, hello) (world, world)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 4. 對分組後的數據進行聚合
    //    (hello, hello, hello) (world, world)
    //    (hello, 3) (world, 2)
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => (word, list.size)
    }

    // 5. 將轉換結果採集到控制台打印出來
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 關閉連接
    sc.stop()

  }
}
