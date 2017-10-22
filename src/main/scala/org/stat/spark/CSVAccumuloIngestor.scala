package org.stat.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.opencsv.CSVParserBuilder
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.lexicoder.impl.ByteUtils
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.util.Tool
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.stat.accumulo.AccumuloUtil._

/**
  * The CSVIngestor takes in a CSV file residing in HDFS, parses each row, converts each column into a value
  * consumable for querying purposes. There are two tables that are written to in order to optimize some of
  * the queries. The first table is the stat_rank table where general queries may be performed against. The
  * stat_rank table will look like a regular table where the schema is similar to:
  *
  * create table stat_rank (id date, kw varchar, mkt varchar, loc varchar, dev varchar, rk int, url varchar purl varchar);
  *
  * The extra purl is the previous day's url stored here to help improve query speed by avoiding extraneous
  * sort/partition which can be slow. Note: this should be tested to see how much, if any, performance gain there is.
  *
  * The second table written to is the stat_signature table. The stat_signature table holds the minhash signature
  * for a particular day worth of statistics where sets are divided by the device type. The minhash signature will
  * be used to generate a jaccard similarity between the different device types. Note: this is an estimate, not
  * the real jaccard similarity where the jaccard similarity is J(A,B) = |A intersect B| / |A union B|
  * The estimate will only use 100 permutations (the typical recommendation), so the expected error will be
  * about O(1/sqrt(100)) or O(0.1). To lower the expected error, the value of k needs to be increased (ie. a value of
  * 400 would lower the expected error to O(1/sqrt(400)) or O(0.05)).
  *
  * @param hadoopConf The hadoop configuration.
  * @param sparkAppName The spark application name as seen in the monitoring page.
  * @param file The file to ingest.
  * @param accumuloUser The Accumulo username.
  * @param accumuloPassword The Accumulo password.
  */
class CSVAccumuloIngestor(hadoopConf: Configuration, sparkAppName: String,
                          file: String, accumuloUser: String, accumuloPassword: String) extends Tool {
  private val logger = Logger.getLogger(classOf[CSVAccumuloIngestor])
  var conf: Configuration = hadoopConf

  @throws[Exception]
  override def run(args: Array[String]): Int = {
    // Create the spark configuration class and register the classes that require serializing
    val sparkConf = new SparkConf()
      .setAppName(sparkAppName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[Pair[Any, Any]], classOf[Text], classOf[LongWritable]))

    // Create the scpark context and the job
    val sc = new SparkContext(sparkConf)
    val job = CSVAccumuloIngestor.configureJob(conf, sparkAppName, file)

    // Create an RDD of CSV lines
    // This will be created lazily and will not be read into memory until a spark action occurs
    // Note: This reads every line at a time, so remember to ignore the first line by looking at the key
    // which is the offset into the file
    val rawCsvPairRdd = sc.newAPIHadoopRDD(
      job.getConfiguration, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Filter out the first row
    val filterFirstRowRawCsvPairRdd = rawCsvPairRdd.filter(csvRow => csvRow._1.get() != 0)

    logger.info("Starting ingest action at " + System.currentTimeMillis())

    // Create two Accumulo BatchWriters, one for the stat_rank and the other for stat_signature
    filterFirstRowRawCsvPairRdd.foreachPartition(csvRowIter => {
      val rankWriter = CSVAccumuloIngestor.getWriter(StatConstants.STAT_RANK_TABLE_NAME, "stat", "stat")
      val sigWriter = CSVAccumuloIngestor.getWriter(StatConstants.STAT_SIGNATURE_TABLE_NAME, "stat", "stat")
      val formatter = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance
      val csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build()

      // Transform and save the CSV data into the two tables: rank and signature
      // Each table is ordered by the crawl date
      csvRowIter.foreach(csvRowKV => {
        println("Processing: " + new String(csvRowKV._2.copyBytes()))

        // The csvRowKV has the LongWritable/Text key value pair -- only need the value
        val columnData = csvParser.parseLine(new String(csvRowKV._2.copyBytes()))
        // Each row has:
        // Keyword, Market, Location, Device, Crawl Date, Rank, URL
        val keyword = columnData(0)
        val market = columnData(1)
        val location = columnData(2)
        val device = columnData(3)
        val crawlDate = formatter.parse(columnData(4))
        val rank = columnData(5).trim.toInt
        val url = columnData(6).trim
        val urlValue = new Value(STRING_LEXICODER.encode(url))

        //println("Crawl Date: " + crawlDate)

        // Write out the ranking URL mutations
        CSVAccumuloIngestor.writeRankMutation(
          rankWriter, keyword, market, location, device, crawlDate, rank, url)

        // Add a day to the crawlDate and add the URL to that date in order to find the changes in the URLs between days
        //        CSVIngestor.writeNextRankMutation(rankWriter, cal, crawlDate, urlValue)

        // Write out the sets mutation based on the device type (not supported yet... need to find the unique prime num)
        //CSVIngestor.writeSignatureMutation(sigWriter, crawlDate, keyword, market, location, device, rank, url)

        // Do the final flush only if the iterator is empty
        CSVAccumuloIngestor.closeIfNecessary(csvRowIter, rankWriter, sigWriter)
      })
    })

    logger.info("Finished ingest action at " + System.currentTimeMillis())
    0
  }

  override def getConf: Configuration = this.conf

  override def setConf(conf: Configuration): Unit = this.conf = conf
}

object CSVAccumuloIngestor extends Configured {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: com.stat.spark.CSVIngestor <appName> <hdfs csv filename> <accumulo username> <accumulo password>")
      sys.exit(1)
    }
    val hadoopConf = new Configuration
    val appName = args(0)
    val filename = args(1)
    val accumuloUsername = args(2)
    val accumuloPassword = args(3)
    hadoopConf.set("user", accumuloUsername)
    hadoopConf.set("password", accumuloPassword)
    val ingestor = new CSVAccumuloIngestor(hadoopConf, appName, filename, accumuloUsername, accumuloPassword)
    ingestor.run(args)
  }

  private def closeIfNecessary(csvRowIter: Iterator[(LongWritable, Text)],
                               rankWriter: BatchWriter, sigWriter: BatchWriter): Unit = {
    if (csvRowIter.isEmpty) {
      rankWriter.flush()
      rankWriter.close()
      sigWriter.flush()
      sigWriter.close()
    }
  }

//  private def writeSignatureMutation(sigWriter: BatchWriter, crawlDate: Date,
//                                     keyword: String, market: String, location: String, device: String,
//                                     rank: Int, url: String): Unit = {
//    // The signature mutation has multiple hashes (100)
//    // The hash method is essentially:
//    //
//    //    h(x) = (ax + b) % c
//    //
//    // where c is the next prime number after the number hashes (101)
//
//    val sigMutation = new Mutation(DATE_LEXICODER.encode(crawlDate))
//    val hashes = MinHash.hashes(keyword, market, location, rank, url)
//
//    // The stat_signature table contains a combiner iterator which will calculate the minhash of
//    // the set which creates the signature -- so the signature is done inside of Accumulo
//    sigMutation.put(StatConstants.SIGNATURE_CF, new Text(device), new Value(concatInts(hashes)))
//    sigWriter.addMutation(sigMutation)
//  }

  private def writeNextRankMutation(rankWriter: BatchWriter, cal: Calendar, crawlDate: Date, urlValue: Value): Unit = {
    cal.setTime(crawlDate)
    cal.add(Calendar.DATE, 1)
    val nextCrawlDate = cal.getTime
    val nextRankMutation = new Mutation(DATE_LEXICODER.encode(nextCrawlDate))
    nextRankMutation.put(StatConstants.CF, StatConstants.PREV_URL_CQ, urlValue)
    rankWriter.addMutation(nextRankMutation)
  }

  private def writeRankMutation(rankWriter: BatchWriter,
                                keyword: String, market: String, location: String, device: String, crawlDate: Date,
                                rank: Int, url: String) : Unit = {
    // The rowid must be unique, so the alternatives are to generate a unique id or use the data itself to generate
    // a unique id. Generating a unique id that has no meaning would mean that if data is to be filtered, an index
    // table would be required which would be a bit much in this context, so generate the rowid from the data itself.
    // The issue with the data is that most the row makes up the uniqueness; only the url is not included
    val rowid = ByteUtils.concat(
      ByteUtils.escape(DATE_LEXICODER.encode(crawlDate)),
      ByteUtils.escape(intToEscapedBytes(rank)),
      ByteUtils.escape(STRING_LEXICODER.encode(keyword)),
      ByteUtils.escape(STRING_LEXICODER.encode(market)),
      ByteUtils.escape(STRING_LEXICODER.encode(location)),
      ByteUtils.escape(STRING_LEXICODER.encode(device)))

    val rankMutation = new Mutation(rowid)
    rankMutation.put(StatConstants.CF, StatConstants.URL_CQ, new Value(STRING_LEXICODER.encode(url)))
    rankWriter.addMutation(rankMutation)
  }


  private def getWriter(tableName: String, accumuloUser: String, accumuloPassword: String) : BatchWriter = {
    getInstance
        .getConnector(accumuloUser, new PasswordToken(accumuloPassword))
        .createBatchWriter(tableName, new BatchWriterConfig)
  }

  private def configureJob(conf: Configuration, appName: String, filename: String) : Job = {
    val job = Job.getInstance(conf, appName)
    FileInputFormat.setInputPaths(job, filename)
    job.setInputFormatClass(classOf[TextInputFormat])
    job
  }
}
