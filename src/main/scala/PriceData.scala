package priceData

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import com.google.cloud.bigquery._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTimeZone

object PriceData {

  val spark = SparkSession.builder().appName("bitcoin").config("spark.master", "local").getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  println(DateTimeZone.getDefault())
  val bucketName = "gs://cs4240-final/bitcoin-cs4240/"
  def fileName(name: String): String = {
    val m = Map(
      ("dominance", "dom.csv"),
      ("cap", "cap.csv"),
      ("usd", "*USD.csv.gz"),
      ("eur", "*EUR.csv.gz"),
      ("jpy", "*JPY.csv.gz"),
      ("eurusd", "EURUSD.csv"),
      ("jpyusd", "JPYUSD.csv"),
      ("test", "coinbaseEUR.csv.gz")
    )
    bucketName + m(name)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val usd = aggPrice("usd")
    val eur = aggPrice("eur")
    val jpy = aggPrice("jpy")


    val finalResult = joinWithOtherData(addAndAverage(usd, eur, jpy))

    finalResult
      .repartition(20)
      .write.mode("append")
      .option("header", "true")
      .format("csv")
      .save("gs://cs4240-final/result")

  }

  def addAndAverage(usd: DataFrame, eur: DataFrame, jpy: DataFrame): DataFrame = {
    val joined = usd.join(eur, Seq("Time"), "fullouter").join(jpy, Seq("Time"), "fullouter")
      .na.fill(0.0, Seq("Price_usd", "count_usd", "Price_eur", "count_eur", "Price_jpy", "count_jpy"))

    joined
      .withColumn("Price", joined("Price_usd") + joined("Price_eur") + joined("Price_jpy"))
      .withColumn("Count", (joined("count_usd") + joined("count_eur") + joined("count_jpy")))
      .withColumn("Price", (col("Price") / col("Count")))
      .drop("Count", "Price_usd", "Price_eur", "Price_jpy", "count_usd", "count_eur", "count_jpy")
  }


  def joinWithOtherData(agg: DataFrame): DataFrame = {
    val domRdd = parseData(spark.sparkContext.textFile(fileName("dominance")))

    val dominanceDF = applyTimeFormat(spark.createDataFrame(domRdd, schema(List("Percent"))))
      .groupBy(col("Time")).avg("Percent").withColumnRenamed("avg(Percent)", "Dominance %")

    val capRdd = parseData(spark.sparkContext.textFile(fileName("cap")))

    val capDF = applyTimeFormat(spark.createDataFrame(capRdd, schema(List("Cap"))))
      .groupBy(col("Time")).avg("Cap").withColumnRenamed("avg(Cap)", "Cap")

    dominanceDF
      .join(capDF, Seq("Time"), "leftouter")
      .join(agg, Seq("Time"), "leftouter")
  }

  def applyTimeFormat(df: DataFrame): DataFrame = {
    df.withColumn("Time", from_unixtime(col("Time").cast(LongType), "yyyy-MM-dd HH:mm"))
  }

  def parseData(data: RDD[String]): RDD[Row] = {
    data.map(_.split(",").toList)
      .map(row => Row.fromSeq(Seq(row.head) ++ row.tail.map(_.toFloat)))
  }

  def aggPrice(name: String): DataFrame = {
    val df = fetchDataAndConvertToDf(name, List("Price", "Amount"), "Price")
    println("Date column added " + name)

    if (name != "usd") {
      val exchangeDf = fetchDataAndConvertToDf(name + "usd", List("Ratio"), "Ratio")
        .withColumn("Ratio", (col("Ratio") / col("count"))).drop("count")
      df.join(exchangeDf, Seq("Time"), "leftouter")
        .withColumn("Price_" + name, col("Price") * col("Ratio")).drop("Ratio", "Price")
        .withColumnRenamed("count", "count_" + name)
    } else {
      df.withColumnRenamed("Price", "Price_" + name)
        .withColumnRenamed("count", "count_" + name)
    }
  }

  def fetchDataAndConvertToDf(name: String, fields: List[String], sumBy: String): DataFrame = {
    println("Fetching ..." + name)
    val rdd = parseData(spark.sparkContext.textFile(fileName(name)))
    println("Data parsed for " + name)
    val df = applyTimeFormat(spark.createDataFrame(rdd, schema(fields))).cache()

    println("Grouping and averaging " + name)
    val count = df.groupBy("Time").count()
    val sum = df.groupBy("Time").sum(sumBy)
    count.join(sum, Seq("Time"), "leftouter")
      .withColumnRenamed("sum(" + sumBy + ")", sumBy)
  }

  def schema(fields: List[String]): StructType = {
    StructType(
      StructField("Time", StringType, false) ::
        fields.map(f => StructField(f, FloatType, false))
    )
  }

}