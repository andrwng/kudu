package org.apache.kudu.spark.kudu

import cats.syntax.either._
import io.circe._
import io.circe.parser._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KuduMetricsReceiver(val metricAddr: String)
    extends Receiver[String](StorageLevel.MEMORY_ONLY) with Runnable {

  val log: Logger = LoggerFactory.getLogger(getClass)

  @transient
  private var thread: Thread = _

  override def onStart(): Unit = {
    thread = new Thread(this)
    thread.start()
  }

  override def onStop(): Unit = {
    thread.interrupt()
  }

  override def run(): Unit = {
    val rawJson = scala.io.Source.fromURL(metricAddr).mkString
    val parseResult: Json = parse(rawJson).getOrElse(Json.Null)
    // TODO(awong): nested-type-support can't come soon enough.
    val entities: Vector[Json] = parseResult.asArray.getOrElse(Vector.empty)
    entities.foreach((entityJson: Json) => {
      val entityType = entityJson.hcursor.downField("type").as[String].getOrElse("")
      val entityId = entityJson.hcursor.downField("id").as[String].getOrElse("")
      val entityAttributes =
        entityJson.hcursor.downField("attributes").focus.getOrElse(Json.Null).toString()
      log.info(entityType)
      log.info(entityId)
      log.info(entityAttributes)

      // TODO(awong): shuffle these into Kudu rows
      // Iterate through the metrics, parsing out whether they're simple
      // metrics or histogram metrics.
      entityJson.hcursor.downField("metrics").as[Vector[Json]].getOrElse(Vector.empty)
        .foreach((metricPair: Json) => {
          val metricName = metricPair.hcursor.downField("name").as[String].getOrElse("")
          val metricVal = metricPair.hcursor.downField("value").as[Long]
          if (metricVal.isRight) {
            log.info(s"$metricName: " + metricVal.getOrElse(0).toString())
          } else {
            val metricMean = metricPair.hcursor.downField("mean").as[Double]
            if (metricMean.isRight) {
              log.info(s"$metricName: " + metricMean.getOrElse(0).toString())
            }
          }

        })
    })
    //log.info(entities.toString())
  }
}
