package org.stat.util

import java.text.SimpleDateFormat

import org.scalatest.{BeforeAndAfter, FlatSpec, FunSpec, FunSuite}

class TestMinHash extends FlatSpec with BeforeAndAfter {
  info("starting")
  before {}

  "hashValue" should "create hash value" in {
    val formatter: SimpleDateFormat = new SimpleDateFormat("YYYY-MM-dd")
    val date = formatter.parse("2017-07-01")
    val hashValue = MinHash.hashValue(new CoefficientPair(62, 92), 3, "search analytics", "US-en", null,1,"support.google.com/webmasters/answer/6155685?hl=en")
    println(hashValue)
    info("Ok")
  }
}
