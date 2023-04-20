package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // when we use inferSchema = true that time spark will accept the nulls value as well.
  // spark will do nullable = true and very careful to use nullable = false you pretty sure about nulls are not in dataset.

  // select the first non-null value
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  ) //.show()
  /*
  Returns the first column that is not null, or null if all inputs are null.
  For example, coalesce(a, b, c) will return a if a is not null, or b if a is null and b is not null, or c if both a and b are null but c is not null.
  * */

  // checking fro nulls --> we can use 'isNull' or 'isNotNull'
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // remove nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing null
  /*     Dropping rows containing any null values.
         ds.na.drop()  */
  // also don't confused with moviesDF.drop("columnName") it will drop the column not row.

  // replace nulls // fill 0 and pass the column names
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")) //.show()

  // powerful version of nulls replace
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating,IMDB_Rating * 10,0.0) as nvl2" // if(first != null) second else third
  ) // if you know SQL then it should understandable otherwise you have to learn spark sql means above code for null.(it's useful in day job)


}

























