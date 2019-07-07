package org.apache.kudu.spark.kudu

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KuduMetricsReceiverTest extends JUnitSuite {
  val log: Logger = LoggerFactory.getLogger(getClass)

  @Test
  def test(): Unit = {
    val appID: String = new Date().toString + math
      .floor(math.random * 10E4)
      .toLong
      .toString

    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
    val metricsAddr = "http://vc1302.halxg.cloudera.com:8051/metrics"

    val ssc = new StreamingContext(conf, Seconds(5))
    val stream =
      ssc.receiverStream(new KuduMetricsReceiver(metricsAddr))
    stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
