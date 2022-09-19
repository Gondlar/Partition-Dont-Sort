package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.{DataFrame,SparkSession}
import de.unikl.cs.dbis.waves.util.operators.{Grouper,NullGrouper}
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.{Bucket,PartitionTree}
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface

/**
  * Implements a splitter which does not split the data but delivers it as a
  * single partition
  */
class NonSplitter(
  input: DataFrame,
  path: String
) extends GroupedSplitter(path) {

  override protected def load(context: Unit): DataFrame = input

  override protected def splitGrouper: Grouper = NullGrouper

  override protected def split(df: DataFrame): Seq[DataFrame] = Seq(df)

  override protected def buildTree(folders: Seq[PartitionFolder]): PartitionTree[String]
    = new PartitionTree(data.schema, Bucket(folders.head.name))
}
