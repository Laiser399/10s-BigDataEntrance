package spark

import org.apache.spark.sql.SparkSession
import org.log4s.getLogger


object App {
  private val logger = getLogger
  private val appName: String = "MyFirstSparkJobInWholeLife"

  private def master: String = sys.env.getOrElse("SPARK_MASTER", "local[10]")

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error(s"Expected 2 program arguments. Got ${args.length}.")
      return
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()

    import spark.implicits._

    val lines = spark
      .read
      .textFile(inputPath)
      .map(x => x + "_somepostfix")
      .rdd

    lines
      .saveAsTextFile(outputPath)
  }
}
