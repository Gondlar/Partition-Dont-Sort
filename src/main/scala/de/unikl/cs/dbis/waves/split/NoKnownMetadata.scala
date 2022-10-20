package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.partitions.PartitionMetadata

import org.apache.spark.sql.DataFrame

/**
  * The NoKnownMetadata Mixin is meant for Splitters where the splits do not
  * carry information for schema modifications. It automatically generated
  * empty metadata for every bucket supplied.
  */
trait NoKnownMetadata extends GroupedSplitter {
  
  /**
    * Build buckets based off the grouped data
    *
    * @param df the data groups grouped by [[splitGrouper]]
    * @return the buckets of data represented as sets of data groupings
    */
  protected def splitWithoutMetadata(df: DataFrame): Seq[DataFrame]

  override protected def split(df: DataFrame): (Seq[DataFrame], Seq[PartitionMetadata]) = {
    val data = splitWithoutMetadata(df)
    (data, Seq.fill(data.size)(PartitionMetadata()))
  }
}
