package dataconf

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql._



/**
  * Created by vbash on 10/9/17.
  */

object ForexProcessor extends App {
  override def main(args: Array[String]): Unit = {


    //old way to get sqlContext
    //val sparkConf = new SparkConf().setAppName("ForexProcessor").setMaster("local[2]")
    // val sc = new SparkContext(sparkConf)
    //val sqlContext = SparkSession.builder().getOrCreate().sqlContext


    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("ForexProcessor")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val sqlContext = spark.sqlContext


    val gbp_eur = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("/Users/vbash/presentations/spark/gbpeur.txt")

    val count = gbp_eur.count()



    println("Number of days in data file: " + count)
    println("Schema", gbp_eur.schema.toString())

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // SELECT TOP(100)
    //    Date,
    //    Close
    // FROM prices
    // WHERE Date > '2015-01-01'
    gbp_eur.select("Date", "Close").filter($"Date" > 20150101).show(100)


    //register Dataframe as a temp table
    gbp_eur.createOrReplaceTempView("gbp_eur")

    //now we can use sql on this dataframe
    spark.sql("SELECT Date, Close FROM gbp_eur WHERE Date > 20150101 LIMIT 100").show

    //try to parse date
    val format = new java.text.SimpleDateFormat("yyyyMMdd")
    println(gbp_eur.rdd.map(row => format.parse(row.getAs[Int]("Date").toString)).first())


    // add two new columns to dataframe: Parsed date and date+30 days
    val gbp_eur_rdd = gbp_eur.rdd.map(row => {
      val ldt = LocalDate.parse(row.getAs[Int]("Date").toString, DateTimeFormatter.ofPattern("yyyyMMdd"))

      val date_time = java.sql.Date.valueOf(ldt)
      val date_time_plus_month = java.sql.Date.valueOf(ldt.plusDays(30))
      Row.fromSeq(row.toSeq ++ Seq(date_time) ++ Seq(date_time_plus_month))
    })

    //create a new dataframe with extended schema
    val gbp_eur_extended = spark.createDataFrame(
      gbp_eur_rdd,
      gbp_eur.schema
        .add("DateTime", org.apache.spark.sql.types.DateType, false)
        .add("DatePlus30", org.apache.spark.sql.types.DateType, false))

    //show first 100 rows
    gbp_eur_extended.show(100)


    // register the extended dataframe as a temp table
    gbp_eur_extended.createOrReplaceTempView("gbp_eur_extended")

    //for each day find days between current and +30 where rate changed > then 10%
    spark.sql("SELECT beginning.DateTime, beginning.Close, ending.DateTime, ending.Close, (ending.Close - beginning.Close)/beginning.Close * 100.0 as change " +
      " FROM gbp_eur_extended as beginning " +
      " JOIN gbp_eur_extended as ending on ending.DateTime between beginning.DateTime and beginning.DatePlus30 " +
      " WHERE abs((beginning.Close - ending.Close)/beginning.Close * 100.0) > 10").show(200)


    //find months with changes >10%
    val badMonths = spark.sql("SELECT YEAR(beginning.DateTime), MONTH(beginning.DateTime), count(*)" +
      " FROM gbp_eur_extended as beginning " +
      " JOIN gbp_eur_extended as ending on ending.DateTime between beginning.DateTime and beginning.DatePlus30 " +
      " WHERE abs((beginning.Close - ending.Close)/beginning.Close * 100.0) > 10 " +
      " GROUP BY YEAR(beginning.DateTime), MONTH(beginning.DateTime) " +
      " ORDER BY 1,2 ")

    badMonths.show

    //save bad months into a file
    badMonths.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/Users/vbash/presentations/spark/badMonths.csv")
  }
}