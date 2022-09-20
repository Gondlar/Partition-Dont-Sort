package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.{DataFrame,SparkSession}
import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.{Bucket,PartitionTree}
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface

/**
  * Implements a splitter which randomly partitions the data
  */
class RandomSplitter(
  input: DataFrame,
  path: String,
  numPartitions: Int
) extends GroupedSplitter(path) with FlatTreeBuilder {

  override protected def load(context: Unit): DataFrame = input

  override protected def splitGrouper: Grouper = NullGrouper

  override protected def split(df: DataFrame): Seq[DataFrame]
    = df.randomSplit(Array.fill(numPartitions)(1)).filter(!_.isEmpty)
}
