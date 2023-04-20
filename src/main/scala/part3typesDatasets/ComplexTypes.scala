package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  // Dates                                                                  6-May-94
  // if somebody already read data as 'String' then what should do?
  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date differences  // we have more functions for 'data_add', 'date_sub'

  // convert into string column into date column

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull) //.show()
  // it will give null on which column where data is 'Null' or different format

  /* Exercise
    1. How do we deal with multiple date formats?
    2. Read the stocks DF and parse the dates
  * */

  // 1 - parse the DF multiple times, then union the small DFs (data cleaning) in real life we have to do that
  // it's not feasible then second date need to be discarded(remove) if 1 % date is not in perfect format.

  // 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))
  stocksDFWithDates.show()

  // Structures
  // 1 Version - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
  // 'struct' will merge two column in one column with array
  //  'getField' will extract field from merged column.

  // 2 - Version with expression strings
  moviesDF.selectExpr("Title", "(US_Gross,Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords =
    moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY of stings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
    // we can access the first word of 'Title' column
  ) // array will used in rarely in real life
}












