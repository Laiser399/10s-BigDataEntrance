package spark

import org.apache.spark.sql.functions.{count, lit, quarter, year}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.log4s.getLogger


object App {
  private val logger = getLogger
  private val appName: String = "MyFirstSparkJobInWholeLife"

  private def master: String = sys.env.getOrElse("SPARK_MASTER", "local[10]")

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      logger.error(s"Expected 4 program arguments. Got ${args.length}.")
      return
    }

    val locationMappingsPath = args(0)
    val questionsOutputPath = args(1)
    val answersOutputPath = args(2)
    val commentsOutputPath = args(3)

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val posts = spark
      .read
      .table("default.posts")
      .select(
        $"Id",
        $"CreationDate",
        $"OwnerUserId"
      )

    val questions = posts
      .where($"PostTypeId" === 1)

    val answers = posts
      .where($"PostTypeId" === 2)

    val comments = spark
      .read
      .table("default.comments")
      .select(
        $"Id",
        $"CreationDate",
        $"UserId".as("OwnerUserId")
      )

    val users = spark
      .read
      .table("default.users")
      .select(
        $"Id",
        $"Location"
      )

    val locationMappings = spark
      .read
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\\")
      .schema(StructType(Array(
        StructField("WeirdLocation", StringType, nullable = false),
        StructField("Country", StringType, nullable = false)
      )))
      .csv(locationMappingsPath)


    val usersWithCountry = users
      .join(locationMappings, users("Location") === locationMappings("WeirdLocation"))
      .select(
        users("Id"),
        locationMappings("Country")
      )


    def aggregate(entries: Dataset[Row]): Dataset[Row] = {
      entries
        .join(usersWithCountry, entries("OwnerUserId") === usersWithCountry("Id"))
        .select(
          usersWithCountry("Country"),
          year(entries("CreationDate")).as("Year"),
          quarter(entries("CreationDate")).as("Quarter")
        )
        .groupBy(usersWithCountry("Country"), $"Year", $"Quarter")
        .agg(count(lit(1)))
    }

    val questionsAggregated = aggregate(questions)
    val answersAggregated = aggregate(answers)
    val commentsAggregated = aggregate(comments)

    def saveAggregation(path: String, aggregation: Dataset[Row]): Unit = {
      aggregation
        .sort($"Country", $"Year", $"Quarter")
        .repartition(1)
        .write
        .format("csv")
        .option("delimiter", "\t")
        .option("quote", "\"")
        .option("escape", "\\")
        .save(path)
    }

    saveAggregation(questionsOutputPath, questionsAggregated)
    saveAggregation(answersOutputPath, answersAggregated)
    saveAggregation(commentsOutputPath, commentsAggregated)
  }
}
