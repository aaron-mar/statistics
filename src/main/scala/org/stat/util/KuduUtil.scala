package org.stat.util

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext

object KuduUtil {
    def kudoContext(sc: SparkContext, masterIp: String): KuduContext = {
        val kuduMasters = Seq(masterIp).mkString(",")
        new KuduContext(kuduMasters, sc)
    }
}
