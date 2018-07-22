package org.apache.kudu.spark.kudu

import java.net.InetAddress

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.kudu.client._

import scala.util.Try

/* This class implements interactions with Spark's DataSource and DataSourceV2.
 *
 */
class KuduSourceProvider extends DataSourceV2
    with DataSourceRegister
    with RelationProvider
    with ReadSupport {

  val TABLE_KEY = "kudu.table"
  val KUDU_MASTER = "kudu.master"
  val OPERATION = "kudu.operation"
  val FAULT_TOLERANT_SCANNER = "kudu.faultTolerantScan"
  val SCAN_LOCALITY = "kudu.scanLocality"
  val IGNORE_NULL = "kudu.ignoreNull"
  val IGNORE_DUPLICATE_ROW_ERRORS = "kudu.ignoreDuplicateRowErrors"
  val SCAN_REQUEST_TIMEOUT_MS = "kudu.scanRequestTimeoutMs"
  val SOCKET_READ_TIMEOUT_MS = "kudu.socketReadTimeoutMs"
  def defaultMasterAddrs: String = InetAddress.getLocalHost.getCanonicalHostName

  def getScanRequestTimeoutMs(parameters: Map[String, String]): Option[Long] = {
    parameters.get(SCAN_REQUEST_TIMEOUT_MS).map(_.toLong)
  }

  def getSocketReadTimeoutMs(parameters: Map[String, String]): Option[Long] = {
    parameters.get(SOCKET_READ_TIMEOUT_MS).map(_.toLong)
  }

  override def shortName(): String = "kudu"

  /**
    * Construct a BaseRelation using the provided context and parameters.
    *
    * @param sqlContext SparkSQL context
    * @param parameters parameters given to us from SparkSQL
    * @return           a BaseRelation Object
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val tableName = parameters.getOrElse(TABLE_KEY,
      throw new IllegalArgumentException(
        s"Kudu table name must be specified in create options using key '$TABLE_KEY'"))
    val kuduMaster = parameters.getOrElse(KUDU_MASTER, defaultMasterAddrs)
    val operationType = getOperationType(parameters.getOrElse(OPERATION, "upsert"))
    val faultTolerantScanner = Try(parameters.getOrElse(FAULT_TOLERANT_SCANNER, "false").toBoolean)
      .getOrElse(false)
    val scanLocality = getScanLocalityType(parameters.getOrElse(SCAN_LOCALITY, "closest_replica"))
    val ignoreDuplicateRowErrors = Try(parameters(IGNORE_DUPLICATE_ROW_ERRORS).toBoolean).getOrElse(false) ||
      Try(parameters(OPERATION) == "insert-ignore").getOrElse(false)
    val ignoreNull = Try(parameters.getOrElse(IGNORE_NULL, "false").toBoolean).getOrElse(false)
    val writeOptions = new KuduWriteOptions(ignoreDuplicateRowErrors, ignoreNull)

    new KuduRelation(tableName, kuduMaster, faultTolerantScanner,
      scanLocality, getScanRequestTimeoutMs(parameters), getSocketReadTimeoutMs(parameters),
      operationType, None, writeOptions)(sqlContext)
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new KuduSourceReader(options.get(TABLE_KEY).orElse(""),
      options.get(KUDU_MASTER).orElse(defaultMasterAddrs),
      options.getBoolean(FAULT_TOLERANT_SCANNER, false),
      getScanLocalityType(options.get(SCAN_LOCALITY).orElse("closest_replica")),
      Option(options.getLong(SCAN_REQUEST_TIMEOUT_MS, 0)),
      Option(options.getLong(SOCKET_READ_TIMEOUT_MS, 0)),
      null)
  }


  private def getOperationType(opParam: String): OperationType = {
    opParam.toLowerCase match {
      case "insert" => Insert
      case "insert-ignore" => Insert
      case "upsert" => Upsert
      case "update" => Update
      case "delete" => Delete
      case _ => throw new IllegalArgumentException(s"Unsupported operation type '$opParam'")
    }
  }

  private def getScanLocalityType(opParam: String): ReplicaSelection = {
    opParam.toLowerCase match {
      case "leader_only" => ReplicaSelection.LEADER_ONLY
      case "closest_replica" => ReplicaSelection.CLOSEST_REPLICA
      case _ => throw new IllegalArgumentException(s"Unsupported replica selection type '$opParam'")
    }
  }
}
