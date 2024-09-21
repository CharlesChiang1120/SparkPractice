import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession

object SparkErrorExample {
  def main(args: Array[String]): Unit = {
    // 配置 Log4j
    val logger = Logger.getLogger(getClass.getName)
    PropertyConfigurator.configure("src/main/resources/log4j.properties")

    // 设置日志级别
    logger.setLevel(Level.ERROR)

    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Error Example")
      .master("local[*]")
      .getOrCreate()

    try {
      // 故意抛出一个异常以演示错误处理
      val result = riskyOperation()
      println(s"Operation Result: $result")
    } catch {
      case e: Exception =>
        // 记录错误到日志文件
        logger.error("An error occurred during the operation", e)
    } finally {
      // 关闭 SparkSession
      spark.stop()
    }
  }

  // 一个可能会抛出异常的方法
  def riskyOperation(): Int = {
    // 故意除以零，抛出 ArithmeticException
    val num = 10
    val zero = 0
    num / zero
  }
}
