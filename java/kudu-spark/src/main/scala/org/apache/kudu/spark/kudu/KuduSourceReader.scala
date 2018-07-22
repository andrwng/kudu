package org.apache.kudu.spark.kudu

import java.math.BigDecimal
import java.util
import java.sql.Timestamp

import org.apache.kudu.Type

import scala.collection.JavaConverters._
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu.SparkUtil._
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType

class KuduSourceReader(private val tableName: String,
                       private val masterAddrs: String,
                       private val faultTolerantScanner: Boolean,
                       private val scanLocality: ReplicaSelection,
                       private[kudu] val scanRequestTimeoutMs: Option[Long],
                       private[kudu] val socketReadTimeoutMs: Option[Long],
                       val sqlContext: SQLContext) extends DataSourceReader
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  var predicates: Array[KuduPredicate] = null
  var columns: StructType = null

  private val context: KuduContext = new KuduContext(masterAddrs, sqlContext.sparkContext,
                                                     socketReadTimeoutMs)

  private val table: KuduTable = context.syncClient.openTable(tableName)

  override def readSchema(): StructType = {
    sparkSchema(table.getSchema)
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    // TODO(awong): create scan tokens
    val tokens: util.List[KuduScanToken] = context.syncClient.newScanTokenBuilder(table).build()
    tokens.asScala.map(token => {
      new KuduDataReaderFactory(token, context)
    }).asJava
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // TODO(awong): store the filters as members and change their types at the end.
    predicates = filters.flatMap(filterToPredicate)
    filters
  }

  override def pushedFilters(): Array[Filter] = {
    // TODO(awong): finish this and above.
    new util.ArrayList[Filter]()
  }

  override def pruneColumns(requiredColumns: StructType) = {
    columns = requiredColumns
  }
    /**
    * Converts a Spark [[Filter]] to a Kudu [[KuduPredicate]].
    *
    * @param filter the filter to convert
    * @return the converted filter
    */
  private def filterToPredicate(filter : Filter) : Array[KuduPredicate] = {
    filter match {
      case EqualTo(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.EQUAL, value))
      case GreaterThan(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.GREATER, value))
      case GreaterThanOrEqual(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, value))
      case LessThan(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.LESS, value))
      case LessThanOrEqual(column, value) =>
        Array(comparisonPredicate(column, ComparisonOp.LESS_EQUAL, value))
      case In(column, values) =>
        Array(inListPredicate(column, values))
      case StringStartsWith(column, prefix) =>
        prefixInfimum(prefix) match {
          case None => Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, prefix))
          case Some(inf) =>
            Array(comparisonPredicate(column, ComparisonOp.GREATER_EQUAL, prefix),
                  comparisonPredicate(column, ComparisonOp.LESS, inf))
        }
      case IsNull(column) => Array(isNullPredicate(column))
      case IsNotNull(column) => Array(isNotNullPredicate(column))
      case And(left, right) => filterToPredicate(left) ++ filterToPredicate(right)
      case _ => Array()
    }
  }
  /**
    * Creates a new in list predicate for the column and values.
    *
    * @param column the column name
    * @param values the values
    * @return the in list predicate
    */
  private def inListPredicate(column: String, values: Array[Any]): KuduPredicate = {
    KuduPredicate.newInListPredicate(table.getSchema.getColumn(column), values.toList.asJava)
  }

  /**
    * Creates a new `IS NULL` predicate for the column.
    *
    * @param column the column name
    * @return the `IS NULL` predicate
    */
  private def isNullPredicate(column: String): KuduPredicate = {
    KuduPredicate.newIsNullPredicate(table.getSchema.getColumn(column))
  }

  /**
    * Creates a new `IS NULL` predicate for the column.
    *
    * @param column the column name
    * @return the `IS NULL` predicate
    */
  private def isNotNullPredicate(column: String): KuduPredicate = {
    KuduPredicate.newIsNotNullPredicate(table.getSchema.getColumn(column))
  }

  /**
    * Creates a new comparison predicate for the column, comparison operator, and comparison value.
    *
    * @param column the column name
    * @param operator the comparison operator
    * @param value the comparison value
    * @return the comparison predicate
    */
  private def comparisonPredicate(column: String,
                                  operator: ComparisonOp,
                                  value: Any): KuduPredicate = {
    val columnSchema = table.getSchema.getColumn(column)
    value match {
      case value: Boolean => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Byte => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Short => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Int => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Long => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Timestamp => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Float => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Double => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: String => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: Array[Byte] => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
      case value: BigDecimal => KuduPredicate.newComparisonPredicate(columnSchema, operator, value)
    }
  }

  /**
    * Returns the smallest string s such that, if p is a prefix of t,
    * then t < s, if one exists.
    *
    * @param p the prefix
    * @return Some(the prefix infimum), or None if none exists.
    */
  private def prefixInfimum(p: String): Option[String] = {
    p.reverse.dropWhile(_ == Char.MaxValue).reverse match {
      case "" => None
      case q => Some(q.slice(0, q.length - 1) + (q(q.length - 1) + 1).toChar)
    }
  }
}

class KuduDataReaderFactory(private val scanToken: KuduScanToken,
                            private val kuduContext: KuduContext) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =  {
    new KuduDataReader(scanToken.intoScanner(kuduContext.syncClient), kuduContext)
  }
}
/**
  * A Spark SQL [[Row]] iterator which wraps a [[KuduScanner]].
  * @param scanner the wrapped scanner
  * @param kuduContext the kudu context
  */
private class KuduDataReader(private val scanner: KuduScanner,
                             private val kuduContext: KuduContext) extends DataReader[Row] {

  private var currentIterator: RowResultIterator = null
  private var rowResult: RowResult = null

  override def next(): Boolean = {
    while ((currentIterator != null && !currentIterator.hasNext && scanner.hasMoreRows) ||
           (scanner.hasMoreRows && currentIterator == null)) {
      if (TaskContext.get().isInterrupted()) {
        throw new RuntimeException("Kudu task interrupted")
      }
      currentIterator = scanner.nextRows()
      // Update timestampAccumulator with the client's last propagated
      // timestamp on each executor.
      kuduContext.timestampAccumulator.add(kuduContext.syncClient.getLastPropagatedTimestamp)
    }
    rowResult = currentIterator.next()
    currentIterator.hasNext
  }

  private def get(rowResult: RowResult, i: Int): Any = {
    if (rowResult.isNull(i)) null
    else rowResult.getColumnType(i) match {
      case Type.BOOL => rowResult.getBoolean(i)
      case Type.INT8 => rowResult.getByte(i)
      case Type.INT16 => rowResult.getShort(i)
      case Type.INT32 => rowResult.getInt(i)
      case Type.INT64 => rowResult.getLong(i)
      case Type.UNIXTIME_MICROS => rowResult.getTimestamp(i)
      case Type.FLOAT => rowResult.getFloat(i)
      case Type.DOUBLE => rowResult.getDouble(i)
      case Type.STRING => rowResult.getString(i)
      case Type.BINARY => rowResult.getBinaryCopy(i)
      case Type.DECIMAL => rowResult.getDecimal(i)
    }
  }

  override def get(): Row = {
    val columnCount = rowResult.getColumnProjection.getColumnCount
    val columns = Array.ofDim[Any](columnCount)
    for (i <- 0 until columnCount) {
      columns(i) = get(rowResult, i)
    }
    Row.fromSeq(columns)
  }

  // TODO(awong): is this right?
  override def close(): Unit = {
    currentIterator.remove()
  }
}
