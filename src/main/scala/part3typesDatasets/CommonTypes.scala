package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a common value to a DF
  // lit will add new column to DF // it work with boolean, numbers, string
  moviesDF.select(col("Title"), lit(47).as("plain_value")) //.show()

  // Booleans
  //val dramaFilter = col("Major_Genre") equalTo  "Drama" // '===' and 'equalsTo' both are fine to use.
  val dramaFilter = col("Major_Genre") === "Drama" // we can use as value
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter // we can combine multiple filter into one and using 'anf' and 'or'

  moviesDF.select("Title").where(preferredFilter)
  // + multiple ways of filtering // we can use multiple filters

  val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // filter on a boolean column and only show 'true' wherever 'good_movie' available
  moviesWithGoodnessFlagDF.where("good_movie") //.show() // means --> where(col("good_movie") === "true")

  // negations
  moviesWithGoodnessFlagDF.where(not(col("good_movie"))) // it will give 'false' movie which is not 'good_movie'

  // Numbers

  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
  // math Operators // if math operator not number then spark will crash
  val moviesAvgRatingDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation  (data science) = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) /* corr is an ACTION */
  // spark will not compute unless ACTION trigger on the code
  // result : 0.4259708986248317 it not correlation between two given column

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization --> initcap, lower, upper
  carsDF.select(initcap(col("Name"))) //.show() // capitalize first letter of the column

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  //val regexString = "volkswagen|vw"
  //carsDF.select("*").where(col("Name").contains(regexString))

  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract") // if you don't want this column then drop it

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ) //.show()

  /*  Exercise
      Filter the cars DF by a list of car names obtained by an API call
      Versions :
        - contains
        - regexes
  * */

  // version 1 - regex
  def getCarNames: List[String] = List("volkswagen", "Mercedes-Benz", "Ford")

  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volkswagen|Mercedes-Benz|Ford
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")
  //.show()

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()
  // if you do functional programming in scala then both exercise will easy for you.


}































