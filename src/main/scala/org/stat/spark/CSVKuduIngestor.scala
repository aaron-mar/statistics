package org.stat.spark

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Tool
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.stat.util.KuduUtil

import scala.collection.JavaConversions._

class CSVKuduIngestor(sc: SparkContext, spark: SparkSession, sparkAppName: String, filename: String, masterIp: String) extends Tool {
    private val logger = Logger.getLogger(classOf[CSVAccumuloIngestor])
    var conf: Configuration = sc.hadoopConfiguration
    val sqlContext: SQLContext = spark.sqlContext
    val kuduContext: KuduContext = KuduUtil.kudoContext(sc, masterIp)

    @throws[Exception]
    override def run(args: Array[String]): Int = {
        // Create the spark configuration class and register the classes that require serializing
        ingest
        1
    }

    def ingest: RDD[Row] = {
        // Create the spark configuration class and register the classes that require serializing
        if (kuduContext.tableExists(CSVKuduIngestor.STAT_RANK_TABLE_NAME)) {
            kuduContext.deleteTable(CSVKuduIngestor.STAT_RANK_TABLE_NAME)
        }

        val primaryKey = Seq("keyword","market","loc","device","crawl_date","rank")
        val tableOptions = new CreateTableOptions()
        tableOptions.setRangePartitionColumns(List("keyword","market","loc","device","crawl_date","rank"))

        kuduContext.createTable(CSVKuduIngestor.STAT_RANK_TABLE_NAME, schema, primaryKey, tableOptions)
        kuduContext.insertRows(statDataFrame, CSVKuduIngestor.STAT_RANK_TABLE_NAME)

        // See if the data is there
        kuduContext.kuduRDD(sc, CSVKuduIngestor.STAT_RANK_TABLE_NAME, Seq("keyword","market","loc","device","crawl_date","rank","url"))
    }

    def statDataFrame: DataFrame = {
        // Load the CSV file
        val csv = spark.read.format("com.databricks.spark.csv")
            .option("delimiter", ",")
            .option("escape", "\"")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(filename)
        // Kudu does not like names with spaces or the word location, so rename the columns
//        val newNames = Seq("keyword","market","loc","device","crawl_date","rank","url")
        val csvRenamed = csv.toDF("keyword","market","loc","device","crawl_date","rank","url")

        // Using the location as part of key, so ensure that location always has a value (no nulls)
        val fixLocationForKuduPrimaryKey = csvRenamed.na.fill("", Seq("loc"))

        // Seems to be duplicates in the dataset, remove the duplicates
        fixLocationForKuduPrimaryKey.dropDuplicates(Seq("keyword","market","loc","device","crawl_date","rank"))
    }

    def schema: StructType =
        StructType(
            StructField("keyword", StringType, false) ::
                StructField("market", StringType, false) ::
                StructField("loc", StringType, true) ::
                StructField("device", StringType, false) ::
                StructField("crawl_date", TimestampType, false) ::
                StructField("rank", IntegerType, false) ::
                StructField("url", StringType, false) :: Nil)

    override def getConf: Configuration = this.conf
    override def setConf(conf: Configuration): Unit = this.conf = conf
}

object CSVKuduIngestor {
    val STAT_RANK_TABLE_NAME: String = "stat_rank"

    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            println("Usage: com.stat.spark.CSVIngestor <appName> <hdfs csv filename> <kudu master IP>")
            sys.exit(1)
        }

        val appName = args(0)
        val filename = args(1)
        val masterIp = args(2)

        val sparkConf = new SparkConf()
            .setAppName(appName)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        // Create the scpark context and the job
        val sc = new SparkContext(sparkConf)
        val spark: SparkSession = SparkSession.builder().appName("StatIngestor").getOrCreate()
        val ingestor = new CSVKuduIngestor(sc, spark, appName, filename, masterIp)
        ingestor.run(args)
    }
}
