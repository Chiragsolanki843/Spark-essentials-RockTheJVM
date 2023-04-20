package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select * from cars where Origin = 'USA'
      |""".stripMargin)

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databaseDF = spark.sql("show databases")
  databaseDF.show()

  // transfer tables from a DB to Spark tables

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

  def transferTables(tableName: List[String], shouldWriteToWarehouse: Boolean = false) = tableName.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    // above code load table from postgres

    // below code will write data into spark tables under the project
    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"))
  // we put under the comment bcz this code will save data again and again into warehouse

  // read DF from loaded Spark tables // means internal warehouse like we save in our project

  // if we comment transfer table then also we need to comment below code.
  val employeesDF2 = spark.read.table("employees").show()
  val departmentsDF2 = spark.read.table("departments").show()
  val titlesDF2 = spark.read.table("titles").show()
  val dept_empDF2 = spark.read.table("dept_emp").show()
  val salariesDF2 = spark.read.table("salaries").show()
  val dept_managerDF2 = spark.read.table("dept_manager").show()


  /*  Exercise
  *
  *   1. Read the movies DF and store it as a Spark table in the rtjvm database.
  *   2. Count how many employees we have in between Jan 1 1999 and Jan 1 2000
  *   3. Show the average salaries for the employees hired in between those dates, grouped by department.
  *   4. Show the name of the best-playing department for employees hired in between those dates.
  * */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //  moviesDF.write
  //    .mode(SaveMode.Overwrite)
  //    .saveAsTable("movies")
  // we comment the code because it will write data again and again so we comment this code.

  // 2
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin) .show()
  // if we use ' '  then spark will consider as an date.

  // 3
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de,salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no=s.emp_no
      |group by de.dept_no
      |""".stripMargin
  ).show()

  // 4
  spark.sql(
    """
      |select  avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de,salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no=s.emp_no
      |and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 100
      |""".stripMargin).show()



}


























