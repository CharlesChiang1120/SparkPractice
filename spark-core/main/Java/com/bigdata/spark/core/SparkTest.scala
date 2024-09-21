import java.awt.Desktop
import java.net.URI
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}

object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.WARN)

    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("Spark SQL Example")
      .master("local[*]") // 在本地运行
      .config("spark.ui.port", "4040") // 指定 Spark UI 端口
      .getOrCreate()

    // 尝试打开 Spark UI
    openSparkUI("http://localhost:4040")

    // 创建 DataFrame
    import spark.implicits._
    val data = Seq(
      ("Alice", 34),
      ("Bob", 45),
      ("Cathy", 29)
    )

    val df: DataFrame = data.toDF("Name", "Age")

    // 创建临时视图
    df.createOrReplaceTempView("people")

    // 执行 SQL 查询
    val resultDF = spark.sql("SELECT * FROM people WHERE Age > 30")
    resultDF.show()

    // 等待用户输入以保持 Spark UI 打开
    println("Press ENTER to stop the Spark application...")
    scala.io.StdIn.readLine() // 等待用户输入

    // 关闭 SparkSession
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
