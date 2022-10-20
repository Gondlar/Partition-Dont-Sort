package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}

/**
  * Implements a splitter which randomly partitions the data
  */
class RandomSplitter(
  numPartitions: Int
) extends GroupedSplitter with FlatTreeBuilder with NoKnownMetadata {

  override protected def splitGrouper: Grouper = NullGrouper

  override protected def splitWithoutMetadata(df: DataFrame): Seq[DataFrame]
    = df.randomSplit(Array.fill(numPartitions)(1)).filter(!_.isEmpty)
}
