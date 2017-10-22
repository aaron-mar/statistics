package org.stat.spark

import java.sql.Timestamp
import java.util.Date

import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.{split, unescape}
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, AccumuloInputFormat, InputFormatBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.mapreduce.Job
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.stat.accumulo.AccumuloUtil.{DATE_LEXICODER, INT_LEXICODER, STRING_LEXICODER, getDateRange}
import org.stat.util.MinHash

import scala.collection.mutable.ListBuffer

/**
  * A service to provide information about the serp dataset.
  *
  * This service connects to Accumulo by default.
  *
  * @param sc The spark context.
  * @param spark The spark session.
  * @param accumuloUsername The accumulo username
  * @param accumuloPassword The accumulo password
  */
class StatService(sc: SparkContext, spark: SparkSession,
                  accumuloUsername: String, accumuloPassword: String,
                  kuduContext: KuduContext,
                  filename: String) {
  private val appName: String = "StatService"

  def getURLOfMostRanksAcrossAllKeywords(rank: Int): String = {
    // Get the values, filtering out the ones less than 10
    // Create a pair RDD of URL and count of 1
    // Run a reduceByKey of the URLs and counts
    // Run a max command to get the max of the counts
    // NOTE: Should filter at Accumulo level by writing a filter iterator
    // so the data does not come back just to get dropped
    val mostRankedUrl = getRddWithAllData
        .filter(d => d._6 > rank)
        .map(d => (d._7, 1))
        .reduceByKey((x, y) =>  x + y)
        .max()(new Ordering[Tuple2[String, Int]]() {
          override def compare(x: (String, Int), y: (String, Int)): Int =
            Ordering[Int].compare(x._2, y._2)
        })
    mostRankedUrl._1
  }

  /**
    * Get the most changed keyword tuple over the dataset period.
    *
    * @return The most changed keyword tuple over the dataset period.
    */
  def getMostChangedRankedOneKeywords: ((String, String, String, String, String), Int) = {
    // Need the set of keywords,
    val mostChangedTuple = getRddWithAllData
        // Only want the first ranks (rank = 1)
        .filter(r => r._6 == 1)
        // Create a key/value pairs of today and yesterday by adding an extra 24 hours to the date
        .flatMap(r=>List(
          ((r._1, r._2, r._3, r._4, r._7, new Timestamp(r._5.getTime)),                   ("today", 1)),
          ((r._1, r._2, r._3, r._4, r._7, new Timestamp(r._5.getTime + (1000*60*60*24))), ("yesterday", 1))))
        // Reduce the days that stay the same by adding the value and change the today/yesterday to 'merged'
        .reduceByKey((t,y) => ("merged", t._2 + y._2))
        // Do not want the extraneous merged and yesterday, so filter them out by keeping only yesterday since
        // yesterday was changed
        .filter(r => r._2._1.equals("yesterday"))
        // Want to do a reduce by key, so the key needs to be unique without the date and the value needs to just a number
        .map(r => ((r._1._1, r._1._2, r._1._3, r._1._4, r._1._5), r._2._2))
        // Add up all the changed values by key in order to figure out which one changed the most
        .reduceByKey((x,y) => x + y)
        // The max value of the tuple is the most changed
        .max()(new Ordering[Tuple2[(String,String,String,String,String),Int]] {
          override def compare(x: ((String,String,String,String,String),Int), y: ((String,String,String,String,String),Int)): Int =
            Ordering[Int].compare(x._2,y._2)
        })

    mostChangedTuple
  }

  /**
    * Get the array of similarities between the device and smartphone by date. The value will be number between
    * 0 and 100 where 0 means there is no similarities and 100 means they are completely the same.
    *
    * This uses a minhash to determine the similarities.
    *
    * @return The list of similarities ordered by date.
    */
  def similarities: Array[(Date, Int)] = {
    val allDataRdd = getRddWithAllData
    val uniqueRows = allDataRdd.map(r => (r._1 + r._2 + MinHash.cs(r._3) + r._6.toString + r._7, 1)).reduceByKey((l1,l2) => l1 + l2).count
    val nextPrimeAfterUniqueRows = MinHash.nextPrime(uniqueRows.toInt)

    // Get the similarities
    allDataRdd
        // Hash the values to create a permutation to prepare for creating the signature, keyed by date and device
        .map(r => ((r._5, r._4), MinHash.hashes(nextPrimeAfterUniqueRows, r._1, r._2, r._3, r._6, r._7)))
        // Create the signature by performing the minhash squashing
        .reduceByKey((h1,h2) => MinHash.signature(h1, h2))
        // At this point, there should be only two signatures (desktop and smartphone), so prepare for the
        // Jaccard similarity by removing the device field so that a further reduceByKey can be done
        .map(r => (r._1._1, ListBuffer(r._2)))
        // Add the signatures into the list buffer to calculate the similarity between them
        .reduceByKey((h1,h2) => h1 ++= h2)
        // Perform the Jaccard similarity calculation
        .map(r => (r._1, if (r._2.length < 2) 0 else MinHash.jaccardSimularity(r._2.head, r._2(1))))
        .sortByKey()
        .collect
  }

  /**
    * Get the array of similarities between the device and smartphone by date. The value will be number between
    * 0 and 100 where 0 means there is no similarities and 100 means they are completely the same.
    *
    * @return The list of similarities ordered by date.
    */
  def actualSimilarities: Array[(Date, Int)] = {
    // Get the similarities
    getRddWithAllData
        // Key by everything except the device type and value is a 1,1 tuple
        // to keep track of duplicates in the reduce phase
        .map(r => ((r._1, r._2, r._3, r._5, r._6, r._7), (1, 1)))
        // Track the duplicates (indicated by the addition of the first value in the tuple) and the totals
        .reduceByKey((v1,v2) => (v1._1 + v2._1, 1))
        // Remove everything except the date from the key
        .map(r => (r._1._4, r._2))
        // Add everything up .. the value will be a tuple of (duplicates + total, total)
        .reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
        // Divide the duplicates by the total (jaccard similarity)
        .map(r => (r._1, ((r._2._1 - r._2._2)*100)/r._2._2))
        .sortByKey()
        .collect()
  }

  /**
    * Gets all the data in the given dataset. The order of the columns are:
    *
    * 1. keyword
    * 2. market
    * 3. location
    * 4. device
    * 5. crawl date
    * 6. rank
    * 7. url
    *
    * @return An RDD with all the data within the given dataset.
    */
  def getRddWithAllData: RDD[(String, String, String, String, Date, Int, String)] = {
    if (filename != null) {
      getRddWithAllDataFromCSV
    } else if (kuduContext != null) {
      getRddWithAllDataFromKudu
    } else {
      getRddWithAllDataFromAccumulo
    }
  }

  /**
    *  Get the data from Accumulo
    * @return The whole dataset from accumulo
    */
  def getRddWithAllDataFromAccumulo: RDD[(String, String, String, String, Date, Int, String)] = {
    val job = configureRankJob()
    val accumuloRdd = sc.newAPIHadoopRDD(job.getConfiguration, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])
    accumuloRdd.map(f => rowTuple(f._1, f._2))
  }

  /**
    * Get the data from the CSV file directly
    * @return The whole dataset from the CSV file
    */
  def getRddWithAllDataFromKudu: RDD[(String, String, String, String, Date, Int, String)] = {
    val kuduRdd = kuduContext.kuduRDD(sc, CSVKuduIngestor.STAT_RANK_TABLE_NAME, Seq("keyword","market","loc","device","crawl_date","rank","url"))
    kuduRdd.map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3), new Date(r.getTimestamp(4).getTime), r.getInt(5), r.getString(6)))
  }

  /**
    * Get the data from the CSV file directly
    * @return The whole dataset from the CSV file
    */
  def getRddWithAllDataFromCSV: RDD[(String, String, String, String, Date, Int, String)] = {
    val csv = spark.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("escape", "\"")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(filename)
    csv.rdd.map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3), new Date(r.getTimestamp(4).getTime), r.getInt(5), r.getString(6)))
  }

  def rowTuple(key: Key, value: Value): (String, String, String, String, Date, Int, String) = {
    val encodedValues = split(key.getRow.copyBytes())
    val crawlDate = DATE_LEXICODER.decode(unescape(encodedValues(0)))
    val rank = INT_LEXICODER.decode(unescape(encodedValues(1)))
    val keyword = STRING_LEXICODER.decode(unescape(encodedValues(2)))
    val market = STRING_LEXICODER.decode(unescape(encodedValues(3)))
    val location = STRING_LEXICODER.decode(unescape(encodedValues(4)))
    val device = STRING_LEXICODER.decode(unescape(encodedValues(5)))
    val url = STRING_LEXICODER.decode(value.get())
    (keyword, market, location, device, crawlDate, rank, url)
  }

  private def configureRankJob(): Job = {
    val job = Job.getInstance(sc.hadoopConfiguration, appName)

    // Initialize the accumulo connector
    AbstractInputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault())
    AbstractInputFormat.setConnectorInfo(job, accumuloUsername, new PasswordToken(accumuloPassword))
    InputFormatBase.setInputTableName(job, StatConstants.STAT_RANK_TABLE_NAME)

    // Note: this is a cheat because real dates should be passed in, but since the desire is across the entire data
    // set, there is no need for an actual range (but Accumulo requires a range of some sort, even if it's empty)
    InputFormatBase.setRanges(job, getDateRange(null, null))

    // Add the whole row iterator to get the whole row as one value
    //InputFormatBase.addIterator(job, new IteratorSetting(50, classOf[WholeRowIterator]))

    job.setInputFormatClass(classOf[AccumuloInputFormat])
    job
  }

  private def configureSignatureJob(): Job = {
    val job = Job.getInstance(sc.hadoopConfiguration, appName)

    // Initialize the accumulo connector
    AbstractInputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault())
    AbstractInputFormat.setConnectorInfo(job, accumuloUsername, new PasswordToken(accumuloPassword))
    InputFormatBase.setInputTableName(job, StatConstants.STAT_SIGNATURE_TABLE_NAME)

    // Note: this is a cheat because real dates should be passed in, but since the desire is across the entire data
    // set, there is no need for an actual range (but Accumulo requires a range of some sort, even if it's empty)
    InputFormatBase.setRanges(job, getDateRange(null, null))

    // Add the whole row iterator to get the whole row as one value
    //InputFormatBase.addIterator(job, new IteratorSetting(50, classOf[WholeRowIterator]))

    job.setInputFormatClass(classOf[AccumuloInputFormat])
    job
  }
}