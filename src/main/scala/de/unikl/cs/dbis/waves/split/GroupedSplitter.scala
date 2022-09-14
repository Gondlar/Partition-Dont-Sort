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
      * This grouper is used for splitting the data
      */
    protected def splitGrouper: Grouper

    /**
      * A Grouper to group the data by. Each grouping represents one kind of data
      * This grouper is used when sorting the data. By default, it is equal to
      * the [[splitGrouper]]
      */
    protected def sortGrouper: Grouper = splitGrouper

    /**
      * Build buckets based off the grouped data
      *
      * @param df the data groups grouped by [[splitGrouper]]
      * @return the buckets of data represented as sets of data groupings
      */
    protected def split(df: DataFrame): Seq[DataFrame]

    /**
      * Sort the data within a bucket
      *
      * @param bucket the bucket grouped by [[sortGrouper]]
      * @return the sorted bucket
      */
    protected def sort(bucket: DataFrame): DataFrame = bucket

    /**
      * Build the [[PartitionTree]] from the given buckets. The list of buckets
      * is ordered in the same order as they were returned by [[split]] and are
      * thus also grouped by [[sortGrouper]]
      *
      * @param buckets the buckets
      */
    protected def buildTree(buckets: Seq[DataFrame]): Unit

    override def partition(): Unit = {
        val types = splitGrouper.group(data)
        types.persist()
        try {
          val buckets = split(types)
          val sorted = buckets.map{ b => 
              sort(sortGrouper.from(splitGrouper, b, data))
          }
          buildTree(sorted)
        } finally types.unpersist()
    }

    protected def data: DataFrame = data(())
}
