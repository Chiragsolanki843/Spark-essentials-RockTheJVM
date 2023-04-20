package part7bigdata

import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import part5lowlevel.RDDs.spark
import org.apache.spark.sql.functions._

object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .appName("Taxi Big Data Application")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val bigTaxiDF = spark.read.load("D:\\Data Engineering\\NYC_taxi_2009-2016.parquet")

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  // directly we use load because data is available in parquet format
  taxiDF.printSchema()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()

  /**
   * Questions:
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   * 2. What are the peak hours for taxi?
   * 3. How are the trips distributed by length? Why are people taking the cab?
   * 4. What are the peak hours for long/short trips?
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   * 6. How are people paying for the ride, on long/short trips?
   * 7. How is the payment type evolving with time?
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   * ( RatecodeID = 1-> credit card, 2-> cash, 3-> no charge, 4-> dispute, 5-> unknown, 6-> voided, 99-> ???)
   */

  // 1
  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)


  // 1b - group by borough
  val pickupsByBorough = pickupsByTaxiZoneDF.groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  // 2
  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  // 3
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )

  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()

  // 4
  val pickupsByHourByLengthDF = tripsWithLengthDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  //  pickupsByHourByLengthDF.show(48)
  // 24 entries for short trips and 24 entries for long trips

  // 5
  //  val pickupDropofPopularityDF = tripsWithLengthDF
  //    .where(not(col("isLong")))
  //    .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
  //    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
  //    .withColumnRenamed("Zone", "Pickup_Zone")
  //    .drop("locationID", "Borough", "service_zone")
  //    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
  //    .withColumnRenamed("Zone", "Dropoff_Zone")
  //    .drop("LocationID", "Borough", "service_zone")
  //    .drop("PULocationID", "DOLocationID")
  //    .orderBy(col("totalTrips").desc_nulls_last)
  //
  //  pickupDropofPopularityDF.show(false)

  // we can also use method
  def pickupDropofPopularity(predicate: Column) = tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("locationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)

  //  pickupDropofPopularity(col("isLong")).show()
  //  pickupDropofPopularity(not(col("isLong"))).show()

  // 6
  val ratecodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID")).agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //  ratecodeDistributionDF.show()

  // 7
  val ratecodeEvolution = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))

  // 8
  val passengerCountDF = taxiDF.where(col("passenger_count") < 3).select(count("*"))
  passengerCountDF.show()
  taxiDF.select(count("*")).show()

  val groupAttemptsDF = taxiDF
    .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"), col("PULocationID"), col("total_amount"))
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")

  val percentGroupAttempt = 0.05 // 5% all ride will group ride
  val percentAcceptGrouping = .3 // 30%
  val discount = 5 // 5 dollar if took group ride
  val extraCost = 2 // 2 dollar extra for private ride
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
  // it will give access for array of double and now we only use [0] index
  // avgCostReduction : double type and also its type of DataSet

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))


  groupingEstimateEconomicImpactDF.show(100)
  groupingEstimateEconomicImpactDF.write
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .csv("src/main/resources/data/EconomicImpact.csv")

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  // 40k/day = 12 million/year!!! // means if we took grouped ride then company save $40k/day so then yearly 1.2cr/year will save
  totalProfitDF.show()
}




























