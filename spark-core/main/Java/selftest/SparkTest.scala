package selftest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.awt.Desktop
import java.net.URI

object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    // Log
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("Spark SQL Example")
      .master("local[*]") // 在本地运行
      .config("spark.ui.port", "4040") // 指定 Spark UI 端口
      .getOrCreate()

    openSparkUI("http://localhost:4040")

    import spark.implicits._
    val data = Seq(
      ("Alice", 34),
      ("Bob", 45),
      ("Cathy", 29)
    )

    val df: DataFrame = data.toDF("Name", "Age")

    df.createOrReplaceTempView("people")

    val resultDF = spark.sql("SELECT * FROM people WHERE Age > 30")
    resultDF.show()

    println("Press ENTER to stop the Spark application...")
    scala.io.StdIn.readLine() // 等待用户输入

    spark.stop()
  }

  def openSparkUI(url: String): Unit = {
    if (Desktop.isDesktopSupported) {
      try {
        Desktop.getDesktop.browse(new URI(url))
      } catch {
        case e: Exception =>
          println(s"Failed to open Spark UI: ${e.getMessage}")
      }
    } else {
      println("Desktop is not supported. Please visit the Spark UI manually at: " + url)
    }
  }
}
