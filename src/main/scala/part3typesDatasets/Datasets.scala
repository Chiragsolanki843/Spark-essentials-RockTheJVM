package part3typesDatasets

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import java.sql.Date
import scala.math.Ordered.orderingToOrdered

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  numbersDF.filter(col("numbers") > 5000)

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  // convert row into Int type

  // dataset of a complex type // fields are need to be same name with the car json file.
  // 1 - define your case class (mostly we use 'case class' for 'DS', 'RDD')
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )

  /*val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))*/

  // 2 - read the DF from the file
  def readDF(fileName: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$fileName")


  // 3 - define an encoder (importing the implicits)

  import spark.implicits._

  val carsDF = readDF("cars.json")
    .withColumn("Year", col("Year").cast(DateType))
  // if 'inferSchema' = 'true' and date is string type then we need to pass our own schema otherwise use above line and 'cast' the date column into 'DateType'
  // implicit val carEncoder=Encoders.product[Car] // in real life defining 'encoder' for every case class it might take long way

  // 4 - Convert the DF to DS
  val carsDS = carsDF.as[Car]
  // encoder automatically fetch from spark implicits

  // DS collection functions
  numbersDS.filter(_ < 100).show()
  // type [] information kept all time and also you can do map, flatmap, fold, reduce, for comprehensions...
  val carNameDS = carsDS.map(car => car.Name.toUpperCase())

  //carNameDS.show

  /*  Exercise
  *
  *   1. Count how many cars we have
  *   2. Count how many cars we have (HP > 140)
  *   3. Average HP for the entire dataset
  * */

  // 1
  val carCount = carsDS.count()
  println(s"Total car in this File : $carCount")
  carsDS.select(count(col("Name"))).show

  // 2 // in the below line '.getOrElse(0L)' because we use 'Option[Long]' in our case class for this column
  // so if nulls value present in that selected column then
  // we have to define if null then 'getOrElse(0L)' also don't forgot to put 'L' if not put then compiler means it's 'Int' type not 'Long' type
  val hpCarCount = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()
  println(s"All Powerful cars with hp more than 140 :- $hpCarCount ")


  // 3
  val hpAvgCarCount = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carCount
  println(s"Average HP for the Entire cars :- $hpAvgCarCount")

  // also use the DF functions!
  carsDS.select(avg(col("Horsepower"))).show()
  // this is easiest way to compute avg for 'Horsepower' column

  // all DataFrame = Dataset[row] means all DF are DS of row

  // Joins

  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long) // in this class we have 'guitars' is an array[Int] type so we pass 'Seq[Int]' and 'Seq' is list type

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayerDS.joinWith(bandsDS, guitarPlayerDS.col("band") === bandsDS.col("id"), "inner")
  // here we use same join condition statement which we use in DF and by default join type is 'inner'
  // if we use 'join' then it will convert into DF and we lose the Type[] info so in this case we use 'joinWith' instead of 'join'
  // the result of the above code is tuple of two Datasets like val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)]
  // in DF we concatenating all rows
  // Dataset is advance version of DF and RDD
  // Cannot up cast id from "BIGINT" to "INT". this line give error if we can't change case class of DS of id to 'Long'
  // because when we use inferSchema = true to read DF so that time DF schema will be 'Long or Double ' type and we create case class that time we used 'Int' that's error came.
  // spark will infer the Schema to Long when we use true.
  // guitarPlayerBandsDS.show(false)

  /*  Exercise : join the guitarsDS and guitarPlayersDS, in an outer join
      (hint : use array_contains)
  *
  * */
  guitarPlayerDS.joinWith(guitarsDS, array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "outer").show(false)
  //val guitarGuitarPlayerDS=guitarsDS.joinWith(guitarPlayerDS,guitarsDS.col("id") === guitarPlayerDS.col(array_contains(col("guitar"),true)),"outer")

  // Grouping DS

  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin).count() // if we use 'count' then will give --> Dataset[(String, Long)] and Long is count of cars in each 'Origin'
    .show()
  //  without using 'count' KeyValueGroupedDataset[String, Car] in our case string is key = Origin and Car is object

  // joins and groups are WIDE transformation, will involve SHUFFLE operations
  // wide transformation means data move across the spark node's of cluster.
  // be careful when use 'join' and 'group' for performance
}



// Datasets
// distributed collection of JVM objects instead of distributed collection of rows(DataFrame)
//  Datasets are powerful
//  Most Useful when
//  - we want to maintain type information
//  - we want clean concise code (special production application)
//  - our filters/transformations are hard to express in DF or SQL

// Avoid when
// - performance is critical : Spark can't optimize transformation. ( it's working in scala code not spark code. when run the application spark need to optimized scala code row by row so performance wise it's slow)
// if you want type[] safety then use 'Dataset' or if you want fast performance then use 'DataFrame'























