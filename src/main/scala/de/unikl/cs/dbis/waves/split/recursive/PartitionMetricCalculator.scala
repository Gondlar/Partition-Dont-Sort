package de.unikl.cs.dbis.waves.split.recursive

import org.apache.spark.sql.{DataFrame,Row}
import de.unikl.cs.dbis.waves.util.PathKey

/**
  * Calculate a PartitionMetric given a DataFrame
  */
trait PartitionMetricCalculator {
  def calc(df: DataFrame): PartitionMetric
  def calcRaw(df: DataFrame): PartitionMetric
  def paths(df: DataFrame): Iterator[PathKey]
}

/**
  * Utility class providing the general flow of PartitionMetic calculations
  */
abstract class AbstractPartitionMetricCalculator extends PartitionMetricCalculator {

  override def calcRaw(df: DataFrame): PartitionMetric
    = df.rdd.mapPartitions(combine).reduce(reduce)

  override def calc(df: DataFrame): PartitionMetric = {
    val metric@(size, present, switch) = calcRaw(df)
    weighByLeafs(switch, df)
    distanceToCenter(present, size)
    metric
  }

  /**
    * Given an ObjectCounter with values between 0 and max, modify it such that
    * its values give the distance to the center of that range.
    *
    * @param max
    * @param counter
    */
  def distanceToCenter(counter: ObjectCounter, max: Int) = {
    counter -= (max/2)
    counter.map(_.abs)
  }

  /**
    * @return The number of optional nodes in the document
    */
  protected def optionalCount: Int

  /**
    * How many datasets does one row represent?
    *
    * @param row the row
    * @return the number
    */
  protected def count(row: Row): Int

  /**
    * The metric used to extract the node value from the row
    *
    * @param row the row
    * @return the metric
    */
  protected def rowMetric(row: Row): Metric

  /**
    * The metric used to determine the number of leafs below each node
    *
    * @param df the DataFrame provided to calc
    * @return the metric
    */
  protected def leafMetric(df: DataFrame): Metric

  /**
  * Calculate statistics for one spark partition
  *
  * @param partition iterator over the partition's entries
  * @return the statistics for this partition
  */
  protected def combine(partition : Iterator[Row]): Iterator[PartitionMetric] = {
    if (partition.isEmpty)
      return Iterator((0, ObjectCounter(optionalCount), ObjectCounter(optionalCount)))
    
    val presentCount = ObjectCounter(optionalCount)
    val switchCount = ObjectCounter(optionalCount)
    var old = ObjectCounter(optionalCount)
    var current = ObjectCounter(optionalCount)
    require(old.size == optionalCount)

    val firstRow = partition.next()
    var rowCount = count(firstRow)
    old <-- rowMetric(firstRow)

    for (row <- partition) {
      rowCount += count(row)
      presentCount += old
      current <-- rowMetric(row)
      old <-- SwitchMetric(current, old)
      switchCount += old
      val tmp = old
      old = current
      current = tmp
    }
    presentCount += old

    Iterator((rowCount, presentCount, switchCount))
  }

  /**
    * Combine the statistics from two partitions into one
    *
    * @param lhs the first partition's stats
    * @param rhs the second partition's stats
    * @return the combined statistics
    */
  protected def reduce(lhs : PartitionMetric, rhs : PartitionMetric) : PartitionMetric = {
    var (rowCount, presentCount, switchCount) = lhs
    rowCount += rhs._1
    presentCount += rhs._2
    switchCount += rhs._3
    (rowCount, presentCount, switchCount)
  }

  /**
    * Multiply the ObjectCounter's values by the number of leafs below each node
    * repectively.
    *
    * @param counter the counter
    * @param df the source DataFrame
    */
  protected def weighByLeafs(counter: ObjectCounter, df: DataFrame): Unit = {
    val leafCount = ObjectCounter(optionalCount)
    leafCount <-- leafMetric(df)
    counter *= leafCount
  }
}