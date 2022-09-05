package de.unikl.cs.dbis.waves.split.recursive

import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.sql.types.StructType
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

/**
  * Calculate the PartitionMetric on a DataFrame that contains presence
  * information precalculated by [[presence]] and holds a count for each such kind
  * 
  * CAUTION: This class cannot be used concurrently!
  */
case class GroupedCalculator(
  schema: StructType,
  presenceCol: String = "presence",
  countCol: String = "count"
) extends AbstractPartitionMetricCalculator {
  private var presenceIndex = -1
  private var countIndex = -1
  
  override def calc(df: DataFrame): PartitionMetric = {
    val schema = df.schema
    presenceIndex = schema.fieldIndex(presenceCol)
    countIndex = schema.fieldIndex(countCol)
    super.calc(df)
  }
  
  override protected def optionalCount: Int = schema.optionalNodeCount
  
  override protected def count(row: Row): Int = row.getLong(countIndex).toInt
  
  override protected def rowMetric(row: Row): Metric
  = PrecalculatedMetric(row, presenceIndex, count(row))
  
  override protected def leafMetric(df: DataFrame): Metric = LeafMetric(schema)
  
  override def paths(df: DataFrame): Iterator[PathKey]
    = ObjectCounter.paths(schema).iterator
}