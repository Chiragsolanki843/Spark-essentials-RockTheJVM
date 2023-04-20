package part7bigdata

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part7bigdata.TaxiApplication.{spark, totalProfitDF}

object TaxiEconomicImpact {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Need 1) Big data source, 2) taxi zones data source, 3) output data destination")
      System.exit(1)
    }

    /*    command line arguments
        1 - big data source
        2 - taxi zones data source
        3 - output data destination
     */
    val spark = SparkSession.builder()
      .appName("Taxi Big Data Application")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val bigTaxiDF = spark.read.load(args(0))

    val taxiZonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    bigTaxiDF.printSchema()

    val percentGroupAttempt = 0.05 // 5% all ride will group ride
    val percentAcceptGrouping = .3 // 30%
    val discount = 5 // 5 dollar if took group ride
    val extraCost = 2 // 2 dollar extra for private ride
    val avgCostReduction = 0.6 * bigTaxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
    val percentGroupable = 289623 * 1.0 / 331893
    // it will give access for array of double and now we only use [0] index
    // avgCostReduction : double type and also its type of DataSet

    val groupAttemptsDF = bigTaxiDF
      .select(round(unix_timestamp(col("pickup_datetime")) / 300).cast("integer").as("fiveMinId"), col("pickup_taxizone_id"), col("total_amount"))
      .groupBy(col("fiveMinId"), col("pickup_taxizone_id"))
      .agg((count("*") * percentGroupable).as("total_trips"), sum(col("total_amount")).as("total_amount"))
      .orderBy(col("total_trips").desc_nulls_last)
      .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
      .drop("fiveMinId")
      .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"))
      .drop("LocationID", "service_zone")


    val groupingEstimateEconomicImpactDF = groupAttemptsDF
      .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
      .orderBy(col("total_trips").desc_nulls_last)
      .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
      .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
      .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))


    val totalEconomicImpactDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
    // 40k/day = 12 million/year!!! // means if we took grouped ride then company save $40k/day so then yearly 1.2cr/year will save

    totalEconomicImpactDF.show()
    totalEconomicImpactDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(args(2))
  }
}

/*
package part7bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import part7bigdata.TaxiApplication.spark

object TaxiEconomicImpact extends App {

  val spark = SparkSession.builder()
    .appName("Taxi Big Data Application")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val bigTaxiDF = spark.read.load("D:\\Data Engineering\\NYC_taxi_2009-2016.parquet")

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  bigTaxiDF.printSchema()

  val percentGroupAttempt = 0.05 // 5% all ride will group ride
  val percentAcceptGrouping = .3 // 30%
  val discount = 5 // 5 dollar if took group ride
  val extraCost = 2 // 2 dollar extra for private ride
  val avgCostReduction = 0.6 * bigTaxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
  val percentGroupable = 289623 * 1.0 / 331893
  // it will give access for array of double and now we only use [0] index
  // avgCostReduction : double type and also its type of DataSet

  val groupAttemptsDF = bigTaxiDF
    .select(round(unix_timestamp(col("pickup_datetime")) / 300).cast("integer").as("fiveMinId"), col("pickup_taxizone_id"), col("total_amount"))
    .groupBy(col("fiveMinId"), col("pickup_taxizone_id"))
    .agg((count("*") * percentGroupable).as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"))
    .drop("LocationID", "service_zone")


  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))


  val totalEconomicImpactDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  // 40k/day = 12 million/year!!! // means if we took grouped ride then company save $40k/day so then yearly 1.2cr/year will save

  totalEconomicImpactDF.show()
}



* */