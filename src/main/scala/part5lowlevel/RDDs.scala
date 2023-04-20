package part5lowlevel

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 10000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - Reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStcoks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1) // drop the first line its header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  // if you read data from CSV then you have to make for separator and header

  val stocksRDD = sc.parallelize(readStcoks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  // val stocksRDD4= stocksDF.rdd // we can convert DF to RDD but it will give RDD[Row] instead of RDD[StockValue]

  import spark.implicits._

  // so convert DF to DS then DS to RDD so type[] information keep save
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // when you transfer RDD to DF then you lose the type info.
  // because DF don't have any type[] , DFs have rows

  val stocksDF1 = stocksRDD2.toDF("company", "date", "price")

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // it will type of DS of Int  DS[Int]
  // DSs keep the type[] information.

  // Transformation

  // counting
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // filter transformation is lazy
  val msCount = msftRDD.count() // count method is ACTION

  // distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy transformation

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min()
  // in above code [StockValue] any one place it should be fine. we can define annotation or
  // when pass Ordering type that time we can define [StockValue] or
  // we can define under the 'sa' and 'sb' as type of [StockValue]

  // reduce
  numbersRDD.reduce(_ + _)


  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive --> grouping will shuffle the data so in RDD its very expensive bcz data will move across the nodes of cluster.

  // Partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  /*
    Repartitioning is EXPENSIVE. Involves Shuffling (Moving data between spark nodes)
    Best practice : partition EARLY (first do repartition), then process the repartition RDDs.
      Size of a partition should be between 10-100MB.
  */

  // coalesce // it will reduce the partitions
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve in SHUFFLING
  // that's means data will not move between entire cluster of nodes.
  // originally we have '30' partitions and we use coalesce with '15' then only 15 partitions have
  // till 15 partition data will stay in same place and another 15 partition data will come into first 15 part.
  // so data will not shuffle as much (its not true SHUFFLING).
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /*
    Exercise

    1. Read the movies.json as an RDD.
    2. Show the distinct genre as an RDD.
    3. Select all the movies in the Drama genre with IMDB_Rating > 6
    4. Show the average rating of movies by genre.
  * */
  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd
  // if we have to select fever column in DF data then manual select the rows and covert into RDD.

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  //  moviesRDD.toDF.show()
  //  //  genresRDD.toDF.show()
  //  goodDramasRDD.toDF.show()

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF().show()
  moviesRDD.toDF().groupBy(col("genre")).avg("rating").show()

}

// Takeaways

// RDDs vs Datasets

// DataFrame == Dataset[Row]

// in common between (DS and RDD)
// collection API : map, flatMap, take, reduce, filter etc.
// combine RDD and DS --> union, count, distinct
// groupBy, sortBy

// RDDs over Datasets (RDD have lots function over the Datasets)
// partition control : repartition, coalesce, partitioner, zipPartitions, mapPartitions
// operation control : checkpoint, isCheckpointed, localCheckpoint, cache
// storage control : cache, getStorageLevel, persist

// Datasets over RDDs
// select and join! (both are powerful method over the RDD)
// Spark planning/optimization before running code


// Collection transformations : map, flatMap, filter

// Actions : count, min, reduce
// also min require implicits ordering for types of the RDD which you want to aggregate.

// Grouping --> when we use groupBy then we got RDD of tuple[key,iterable] // grouping is expensive operation and it will involves in Shuffling

// Partitioning --> rdd.repartition(40) also it will expensive operation and involve in shuffling.
// rdd.coalesce(10) is NOT a full shuffle. but it will reduces number of partition.

