package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()


  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  *   Reading a DF :
  *   - format
  *   - schema or inferSchema = true
  *   - path
  *   - zero or more options
  *
  *
  * */

  val carsDF = spark.read
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed (ignore faulty rows), permissive-->if we don't specified  the mode implicitly will be permissive(default)
    .json("src/main/resources/data/cars.json")
  //.option("path","src/main/resources/data/cars.json") this also fine.
  //.load("") if we have data in the S3 bucket then we have to give path here

  //  carsDF.show() // loading DF and DF are lazy until we call 'ACTION'
  // failFast --> if we have got other dataType data then it will throw the Exception at run time.

  // The DROPMALFORMED mode ignores corrupted records. The meaning that,
  // if you choose this type of mode, the corrupted records won't be list.
  // means suppose we have IntegerType Data in rows and we use DropMalformed mode then which ever data is not integer type it should not show.

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*  Writing DFs
  *   - format
  *   - save mode = overwrite, append, ignore, errorIfExists
  *   - path
  *   - zero or more options
  *
  * */
  carsDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    // .option("path", "src/main/resources/data/cars_duplicate.json") we can use this or below. if we use this then we need to 'save()' black
    .save("src/main/resources/data/cars_duplicate.csv")
  // it will give data in only partition if i working in big data then it should be give numbers of partition


  carsDF.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_duplicate.txt")

  // JSON flags
  spark.read
    //.format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, // this date format and schema date format wrong then spark will put nulls to all. and its mostly use in data science and data processing.
    .option("allowSingleQuotes", "true") // as per the file if json file have single quote then use this. otherwise double quote(default) need to use.
    .option("compression", "uncompressed") // save lot of space if we use compression like --> bzip2, gzip, lz4, snappy, deflate, uncompressed (by default)
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("Symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    //.format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // if you pass schema then schema column name and file column name must be same. when use header true. also if pass the schema then this line will ignore.
    .option("sep", ",") // sometime we got ';' or '<TAB>' and default value is ','
    .option("nullValue", "") // if got null in rows of column then put '' empty string
    .csv("src/main/resources/data/stocks.csv")
  //.load("src/main/resources/data/stocks.csv")

  // Parquet (open source compressed binary data storage format, optimized fast reading of column) also its default storage for DataFrame
  carsDF.write
    //.format("parquet") no need to mention its default
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")
  //.parquet("src/main/resources/data/cars.parquet") // parquet is default format so no need to mention
  // always use parquet for writing a data it much more less data consume

  // Text Files
  spark.read.text("src/main/resources/data/sampleTextFile.txt")

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val pass = "docker"

  val employeesDF = spark.read
    .format("jdbc") // connection type
    .option("driver", driver) // driver will available for every database will can google out of it
    .option("url", url) // url with database name
    .option("user", user) // user name
    .option("password", pass) // password
    .option("dbtable", "public.employees") // table name with schema name in our case 'public'
    .load()

  // employeesDF.show()

  /*
      Exercise : read the movies DF, then write it as
       -  tab-separated values file (csv with TAB separator)
       -  snappy Parquet
       -  table public.movies in the Postgres DB

  * */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")



  // Exercise 1
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/moviesfile.json")

  // Exercise 2
  moviesDF.write.parquet("src/main/resources/data/movies.parquet")

  // Exercise 3
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", pass)
    .option("dbtable", "public.movies")
    .save() // if you want to save data then use 'save()' and load data then 'load()'


}




















