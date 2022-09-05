package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import javax.xml.crypto.Data

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
      * A Column function to group the data by. Each grouping represents one
      * kind of data
      *
      * @see [[de.unikl.cs.dbis.waves.util.operators.definitionLevels]]
      *      and [[de.unikl.cs.dbis.waves.util.operators.presence]]
      * @return the column function
      */
    protected def grouper: StructType => Column

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
        val table = data
        val types = table.groupBy(grouper(table.schema)).count()
        types.persist()
        try buildTree(split(types).map(sort _))
        finally types.unpersist()
    }

    protected def data: DataFrame = data(())
}
