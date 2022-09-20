package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}
import de.unikl.cs.dbis.waves.util.operators.TempColumn

import org.apache.spark.sql.functions.monotonically_increasing_id

/**
  * Implements a splitter which partitions the data into a given number of
  * approximately even-sized buckets
  */
class EvenSplitter(
  input: DataFrame,
  path: String,
  numPartitions: Int
) extends GroupedSplitter(path) with FlatTreeBuilder {

  override protected def load(context: Unit): DataFrame = input

  override protected def splitGrouper: Grouper = NullGrouper

  override protected def split(df: DataFrame): Seq[DataFrame] = {
    val tmp = TempColumn.apply("part")
    val withPartitionId = df.withColumn(tmp, monotonically_increasing_id().mod(numPartitions))
    0.until(numPartitions)
     .map(n => withPartitionId.where(tmp === n).drop(tmp.col))
  }
}
