package org.stat

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.stat.spark.{CSVKuduIngestor, StatService}

class StatMain(sc: SparkContext, spark: SparkSession) {
    def run(filename: String, masterIp: String = "localhost:7051"): Unit = {
        println()
        println("This program ingests the statistical CSV data and perform a few analytics on it")
        println()
        println("The data store used will be kudu: https://kudu.apache.org/")
        println("Kudu was selected since it able to ingest quickly, perform fast analytics and has good Spark integration")
        println("")
        println("Spark is used to perform the analytics")
        println()
        println("The stages of the program are:")
        println(" 1. Ingest the CSV data into Kudu using Spark")
        println(" 2. Find the URL that has the most ranks across all keywords over the period")
        println(" 3. Find the keywords where the rank 1 URL changes the most")
        println(" 4. Find how similar the sets across devices")
        println()
        println("Ingesting the data...")
        println()
        val ingestor = new CSVKuduIngestor(sc, spark, "statapp", filename, masterIp)
        val kuduRdd: RDD[Row] = ingestor.ingest
        val kuduContext = ingestor.kuduContext

        println()
        println("Ingestion Done.")
        println()
        println("Using Kudu data store for analytics..")
        println()
        println("Finding the URL that has the most ranks across all keywords...")
        val statService: StatService = new StatService(sc, spark, null, null, ingestor.kuduContext, null)
        val url = statService.getURLOfMostRanksAcrossAllKeywords(10)
        println()
        println("The most ranked URL below 10: " + url)
        println()
        println("Finding the keywords where the rank 1 URL changed the most...")
        val changed = statService.getMostChangedRankedOneKeywords
        println()
        println("The most changed is " + changed._1 + ": ")
        println()
        println("    Keyword: " + changed._1._1)
        println("    Market: " + changed._1._2)
        println("    Location: " + changed._1._3)
        println("    Device: " + changed._1._4)
        println("    URL: " + changed._1._5)
        val formatter = new SimpleDateFormat("YYYY-MM-dd")

        println()
        println("Finding the estimated similarity between devices over time...")
        val startEstimate = System.currentTimeMillis()
        val similarity = statService.similarities
        val endEstimate = System.currentTimeMillis()
        println()
        println("Time taken for estimate: " + (endEstimate - startEstimate) + "ms")
        println()
        println("Similarity is measured as a percentage of how close they sets are where 0 means " +
            "they are not similar at all and 100 means they are exactly the same")
        println()
        println("The estimated similarity is measured using an approximation minhash algorithm, " +
            "so the results are estimates where the estimate error is about 10%")
        println()
        println("The estimated similarities between the device types over the period is (higher is more similar):")
        println()
        similarity.foreach(r => {
            println(formatter.format(r._1) + ": " + r._2 + "% " + (for (i <- 1 to r._2) yield ".").toArray.mkString(""))
        })
    }

    def similarity(filename: String): Unit = {
        val statService: StatService = new StatService(sc, spark, null, null, null, filename)
        println()
        println("Finding the actual similarity between devices over time...")
        val startActual = System.currentTimeMillis()
        val actualSimilarity = statService.actualSimilarities
        val endActual = System.currentTimeMillis()
        println()
        println("Time taken for actual: " + (endActual - startActual) + "ms")
        println()
        println("The similarities between the device types over the period is (higher is more similar):")
        println()
        val formatter = new SimpleDateFormat("YYYY-MM-dd")
        actualSimilarity.foreach(r => {println(formatter.format(r._1) + ": " + r._2 + "% " + (for (i <- 1 to r._2) yield ".").toArray.mkString(""))})
        println()
        println("Done.")
    }
}
