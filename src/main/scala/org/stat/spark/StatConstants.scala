package org.stat.spark

import org.apache.hadoop.io.Text

object StatConstants {
  val STAT_RANK_TABLE_NAME = "stat_rank"
  val STAT_SIGNATURE_TABLE_NAME = "stat_signature"
  val CF = new Text("")
  val SIGNATURE_CF = new Text("dev")

  val KEYWORD_CQ = new Text("kw")
  val MARKET_CQ = new Text("mkt")
  val LOCATION_CQ = new Text("loc")
  val DEVICE_CQ = new Text("dev")
  val DATE_CQ = new Text("dt")
  val RANK_CQ = new Text("rk")
  val URL_CQ = new Text("url")
  val PREV_URL_CQ = new Text("purl")
}
