package de.unikl.cs.dbis.waves.split.recursive

import org.apache.spark.sql.{DataFrame,Row}
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

/**
  * Calculate the PartitionMetric on a DataFrame that directly contains the source rows
  * 
  * CAUTION: This class cannot be used concurrently!
  */
case class RowwiseCalculator() extends AbstractPartitionMetricCalculator {
  
  private var optional = 0
  
  override def calc(df: DataFrame): PartitionMetric = {
    optional = df.schema.optionalNodeCount
    super.calc(df)
  }
  
  override protected def optionalCount: Int = optional
  
  override protected def count(row: Row): Int = 1
  
  override protected def rowMetric(row: Row): Metric = PresentMetric(row)
  
  override protected def leafMetric(df: DataFrame): Metric
  = LeafMetric(df.schema)
  
  override def paths(df: DataFrame): Iterator[PathKey]
    = ObjectCounter.paths(df.schema).iterator
}
