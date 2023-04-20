package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object Aggregations extends App {
  // aggregation and grouping is wide transformation means data will shuffle across the nodes of clusters.
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genreCountDF = moviesDF.select(count(col("Major_Genre")).as("Total_Unique_Genre")) // all the values except nulls
  //genreCountDF.show() // when we use single column then it will give except nulls

  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls
  moviesDF.select(count(col("*"))) // above and this line same output for all records in file

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))) // except null value
  //moviesDF.select(col("Major_Genre")).distinct().show() // it will show will null value.

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) // it will give approx data not accurate rows

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  val maxRatingDF = moviesDF.select(max(col("IMDB_Rating")))
  moviesDF.selectExpr("max(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")), // it will use in data science for avg
    stddev(col("Rotten_Tomatoes_Rating")) // it will use also in data science for far value
  )

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count() // select count(*) from moviesDF group by Major_Genre
  // it will give including nulls

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating").desc) // still it will including nulls
  //aggregationsByGenreDF.show()

  /*
  *   Exercises
  *
  *   1. Sum up ALL the profits of ALL the movies in the DF
  *   2. Count how many distinct directors we have
  *   3. Show the mean and standard deviation of US gross revenue for the movies
  *   4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
  * */


  // 1
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
  //.show()
  // 2
  val distinctDirectorDF = moviesDF.select(countDistinct(col("Director")))
  //distinctDirectorDF.show()

  //3
  val meanandstdDF = moviesDF.agg(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  // 4
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("avg_rating"),
      sum("US_Gross").as("Total_US_Gross")
    ).orderBy((col("avg_rating")).desc_nulls_last)
    .show()
}































