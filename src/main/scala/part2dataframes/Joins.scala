package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  // Combine data from multiple DataFrames
  // - one (or more) column from table 1 (left) is compared with one (or more) column from table 2 (right)
  // if the condition passes, rows are combined
  // non-matching rows are discarded
  // Wide Transformations (read : expensive!)
  // if you want to RUN this program then you have to start Docker and connect PostgreSQL

  // inner Join --> 'inner'   // by default no need to mention
  // leftOuter Join --> 'left_outer'
  // rightOuter Join --> 'right_outer'
  // fullOuter Join --> 'outer'
  // leftSemi Join --> 'left_semi'
  // leftAnti Join -->  'left_anti'


  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins (most of time in work will do inner join) also no need to pass argument for 'inner' its default value for join
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id") // for the big data we can extract join condition.
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  //guitaristsBandsDF.show() --> it will give output like both table matched data.

  // out Join
  // left outerJoin = everything in th inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer") //.show()

  // right outerJoin = everything in th inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer") //.show()

  // full outerJoin = everything in th inner join + all the rows in the BOTH table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer") //.show()
  // outer will do first 'inner' join then 'left_outer' then 'right_outer' we can see both side nulls which data is not matched.

  // semi-joins =everything in the left DF for which there is row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi") //.show
  // --> it will give output like left table data only which is matched to right table(right table data will not show).

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti") //.show
  // it will show data which is not matched with right table. so left table data wil show which is not matched.

  // thing to bear in mind
  //guitaristsBandsDF.select("id", "band").show() // this line code will crash because reference id ambiguous
  // both DFs have id column so spark will confused which id to pick and show.

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
  // now right side DFs col-'id' we renamed with 'band' and join with  left side DFs 'band' column.
  // 'band' columns only one's will appears in resulting DFs

  // option 2 - drop the duplicate column
  guitaristsBandsDF.drop(bandsDF.col("id"))
  // guitaristsBandsDF.drop(col("id")) --> spark will again confused bcz DFs have two 'id' column in both DFs

  // options 3 -rename the offending column and keep the data (no need to use bcz still get dup. data)
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // Using complex types (joins using array[])
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars,guitarId)"))


  /*  Exercise

      - show all employees and their max salary
      - show all employees who were never manager
      - find the job titles of the best paid 10 employees in the company
  * */
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val pass = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", pass)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  //1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  //employeesSalariesDF.show()

  // 2
  val empNeverManagersDF = employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"), "left_anti")
  //empNeverManagersDF.show()

  //3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary") desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  //bestPaidJobsDF.show()


}




















