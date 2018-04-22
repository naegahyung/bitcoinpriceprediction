package hey
import org.apache.log4j.{Level, Logger}
import java.io.FileInputStream

import com.google.api.services.bigquery.model.QueryRequest
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object hey {


  val spark = SparkSession.builder().appName("bitcoin").getOrCreate()

  val conf = spark.sparkContext.hadoopConfiguration
  conf.setBoolean("google.cloud.auth.service.account.enable", true)
  conf.set("google.cloud.auth.service.account.json.keyfile", "/Users/jin/CS4240/final/server-e57169c1f37a.json")
  conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  conf.set("fs.gs.project.id", "server-177600")

  val fileInputStream = new FileInputStream("server-e57169c1f37a.json")
  val credentials = GoogleCredentials.fromStream(fileInputStream)
  val bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("server-177600").build().getService()
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    import spark.implicits._

    val usd = aggregateUSD(bigquery)
    val eur = aggregateEUR(bigquery)
    val jpy = aggregateJPY(bigquery)
    val eurusd = getExchangeRate(bigquery, "eur_usd")
    val jpyusd = getExchangeRate(bigquery, "jpy_usd")
    val finalEur = eur.join(eurusd, Seq("Time"), "leftouter")
      .select($"Time", ($"Price" * $"Ratio").as("Price_eur"), $"Count".as("Count_eur")).cache()
    val finalJpy = jpy.join(jpyusd, Seq("Time"), "leftouter")
      .select($"Time", ($"Price" * $"Ratio").as("Price_jpy"), $"Count".as("Count_jpy")).cache()
    val finalResult = joinWithOtherData(addAndAverage(usd, finalEur, finalJpy), bigquery)
    finalResult.show
    /*finalResult
    .repartition(4)
    .write.mode("append")
    .option("header", "true")
    .format("csv")
    .save("gs://cs4240-final/result")*/
  }

  def joinWithOtherData(agg: DataFrame, bigquery: BigQuery): DataFrame = {
    val otherDataQuery = QueryJobConfiguration
      .newBuilder("SELECT " +
        "FORMAT_TIMESTAMP('%Y-%m-%d %H', cap.timestamp) as timestamp, cap.amount as amount, dom.percent as percent" +
        "FROM bitcoin_cs4240.market_cap as cap " +
        "JOIN bitcoin_cs4240.dominance as dom " +
        "ON cap.timestamp = dom.timestamp ")
      .setUseLegacySql(false)
      .build()
    val result = bigquery.query(otherDataQuery).iterateAll()
      .map(row => {
        (row.get(0).getValue.toString, (row.get(1).getValue.toString.toFloat, row.get(2).getValue.toString.toFloat, 1))
      })
      .toSeq
    val rdd = spark.sparkContext.parallelize(result)
      .reduceByKey((e1, e2) => (e1._1 + e2._1, e1._2 + e2._2, e1._3 + e2._3))
      .mapValues(e => (e._1 / e._3, e._2 / e._3))
      .map(row => Row.fromSeq(Seq(row._1, row._2._1, row._2._2)))
    val df = spark.createDataFrame(rdd, schema(List("Cap", "Percent")))
    df.join(agg, Seq("Time"), "rightouter")
      .select(col("Time").cast(TimestampType), col("Price"), col("Percent"), col("Cap"))
  }

  def addAndAverage(usd: DataFrame, eur: DataFrame, jpy: DataFrame): DataFrame = {
    val joined = usd.join(eur, Seq("Time"), "fullouter").join(jpy, Seq("Time"), "fullouter")
      .na.fill(0.0, Seq("Price", "Count", "Price_eur", "Count_eur", "Price_jpy", "Count_jpy"))
    joined
      .select(joined("Time"),
        (joined("Price") + joined("Price_eur") + joined("Price_jpy")).as("Price"),
        (joined("Count") + joined("Count_eur") + joined("Count_jpy")).as("Count"))
      .select(col("Time"), (col("Price") / col("Count")).as("Price"))
  }

  def getExchangeRate(bigquery: BigQuery, table: String): DataFrame = {
    val queryConfig = QueryJobConfiguration
      .newBuilder(formQuery(table, "ratio"))
      .setUseLegacySql(false)
      .build()
    val result: Seq[(String, Float)] = bigquery.query(queryConfig).iterateAll()
      .map(row => (row.get(0).getValue.toString, row.get(1).getValue.toString.toFloat))
      .toSeq
    val rdd: RDD[(String, Float)] = spark.sparkContext.parallelize(result)
    val rates = rdd.map(pair => Row.fromSeq(Seq(pair._1, pair._2)))
    spark.createDataFrame(rates, schema(List("Ratio"))).cache()
  }

  def aggregateJPY(bigquery: BigQuery): DataFrame = {
    val query = formQuery("jpy_bitflyer", "price") +
      "UNION ALL " +
      formQuery("jpy_coincheck", "price") +
      "ORDER BY time DESC LIMIT 10000"
    aggregateTable(bigquery, query)
  }

  def aggregateEUR(bigquery: BigQuery): DataFrame = {
    val query = formQuery("eur_coinbase", "price") +
      "UNION ALL " +
      formQuery("eur_kraken", "price") +
      "ORDER BY time DESC LIMIT 10000"
    aggregateTable(bigquery, query)
  }

  def aggregateUSD(bigquery: BigQuery): DataFrame = {
    val query = formQuery("usd_coinbase", "price") +
      "UNION ALL " +
      formQuery("usd_kraken", "price") +
      "UNION ALL " +
      formQuery("usd_bitfinex", "price") +
      "UNION ALL " +
      formQuery("usd_bitstamp", "price") +
      "ORDER BY time DESC  LIMIT 10000"
    aggregateTable(bigquery, query)
  }

  def aggregateTable(bigquery: BigQuery, query: String): DataFrame = {
    val queryConfig = QueryJobConfiguration
      .newBuilder(query)
      .setUseLegacySql(false)
      .build()
    val fetched = bigquery.query(queryConfig).iterateAll()
      .map(row => (row.get(0).getValue.toString, (row.get(1).getValue().toString.toFloat, 1.toFloat))).toSeq
    val rdd: RDD[(String, (Float, Float))] = spark.sparkContext.parallelize(fetched)
    val averaged = groupByTime(rdd)
    spark.createDataFrame(averaged, schema(List("Price", "Count"))).cache()
  }

  def groupByTime(rdd: RDD[(String, (Float, Float))]): RDD[Row] = {
    rdd
      .reduceByKey((el1, el2) => (el1._1 + el2._1, el1._2 + el2._2))
      .map(pair => Row.fromSeq(Seq(pair._1, pair._2._1, pair._2._2)))
  }

  def schema(fields: List[String]): StructType = {
    StructType(
      StructField("Time", StringType, false) ::
        fields.map(f => StructField(f, FloatType, false))
    )
  }

  def formQuery(tableName: String, field: String): String = {
    "SELECT " +
      "FORMAT_TIMESTAMP('%Y-%m-%d %H', timestamp) as time, " + field +
      " FROM bitcoin_cs4240." + tableName + " "
  }
}
