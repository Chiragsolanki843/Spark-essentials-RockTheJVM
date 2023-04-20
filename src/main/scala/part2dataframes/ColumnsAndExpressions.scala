package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // carsDF.show()

  // DF are immutable
  // Columns
  //  val firstColumns = carsDF.col("Name")

  // selecting (Projecting) // means projecting new DF with less data.
  val carNameDF = carsDF.select(col("Name"), col("Cylinders"))

  // various select methods

  import spark.implicits._

  // various way to select 'column' form dataset
  carsDF.select( // we can use any one from below and 'col' and 'column' both doing same
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year") // it will give another DF

  // "select" --> is narrow transformation we select the data from different column and show the data in same nodes so its narrow transformation
  // input partition and output partition are same so its called narrow transformation


  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs") // returns column object
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2 // returns column object

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_KG"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_KG_2")
  )

  // selectexpr --> we can do our arithmetic calculation directly in string at "selectexpr" block.
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2) //.show() // this is expression associated with first column
  // it will give whole DF columns with "Weight_in_kg_3" in last and second is expression like calculating then give data the first column

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") === "USA") // if condition is equal then use '===' with both function 'filter' and 'where'
  // above both are same to use

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  // val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and (col("Horsepower") > 150))
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  // we can use and operator so 'filter' not use two times, also 'and' method is infix so no need parentheses
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150 ")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Year").distinct()

  // println(carsDF.select("Year").distinct().count())

  /*
      Exercises

      1. Read the movies DF and select 2 columns of your choice (Title, Release_Date)
      2. Create another column summing up the the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
      3. Select all COMEDY movies from Major_Genre with IMDB rating above 6

      Use as many versions as possible

   */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  //moviesDF.select("Title", "Release_Date") --> this is also working
  moviesDF.select(col("Title"), col("Release_Date"))

  // 2
  val totalProfitDF = moviesDF.withColumn("Total_Profit", col("US_Gross") + col("Worldwide_Gross"))
  val total = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Overall Profit")
  )

  val total2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  val total3 = moviesDF.select("Title",
    "US_Gross",
    "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))


  //3
  moviesDF.createOrReplaceTempView("moviesDF")
  spark.sql(
    """
      |select * from moviesDF where moviesDF.Major_Genre = "Comedy" and IMDB_Rating > 6
      |""".stripMargin) //.show()


  moviesDF.select("Title", "Major_Genre", "IMDB_Rating")
    .filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6) //.show()

  moviesDF.select("Title", "Major_Genre", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6").show()

}



















