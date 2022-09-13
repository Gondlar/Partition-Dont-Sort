package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.Grouper

/**
  * Implements splitting based off groups of strucuturally identical rows.
  * 
  * The data is read and then grouped using [[grouper]]. The results are
  * processed by [[split]]. [[sort]] is called for every bucket returned and 
  * finally the sorted buckets are passed to [[buildTree]] in the same order as
  * split returned them.
  */
abstract class GroupedSplitter extends Splitter[Unit] {

    /**
      * A Grouper to group the data by. Each grouping represents one kind of data
      * 
      * @return the column function
      */
    protected def grouper: Grouper

    /**
      * Build buckets based off the grouped data
      *
      * @param df the data groups
      * @return the buckets of data represented as sets of data groupings
      */
    protected def split(df: DataFrame): Seq[DataFrame]

    /**
      * Sort the data within a bucket
      *
      * @param bucket the bucket
      * @return the sorted bucket
      */
    protected def sort(bucket: DataFrame): DataFrame = bucket

    /**
      * Build the [[PartitionTree]] from the given buckets. The list of buckets
      * is ordered in the same order as they were returned by [[split]]
      *
      * @param buckets the buckets
      */
    protected def buildTree(buckets: Seq[DataFrame]): Unit

    override def partition(): Unit = {
        val types = grouper.group(data)
        types.persist()
        try buildTree(split(types).map(sort _))
        finally types.unpersist()
    }

    protected def data: DataFrame = data(())
}
