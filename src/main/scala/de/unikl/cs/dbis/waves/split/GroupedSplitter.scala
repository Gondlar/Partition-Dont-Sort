package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.{SparkSession, DataFrame}
import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.sort.{Sorter,NoSorter}
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.operators.Grouper
import de.unikl.cs.dbis.waves.util.operators.AbstractGrouper
import de.unikl.cs.dbis.waves.util.functional._
import scala.concurrent.{Future, ExecutionContext, blocking, Await}
import scala.concurrent.duration.Duration
import org.apache.hadoop.fs.Path

/**
  * Implements splitting based off groups of strucuturally identical rows.
  * 
  * The data is read and then grouped using [[grouper]]. The results are
  * processed by [[split]]. [[sort]] is called for every bucket returned and 
  * finally the sorted buckets are passed to [[buildTree]] in the same order as
  * split returned them.
  */
abstract class GroupedSplitter(
  protected var sorter: Sorter = NoSorter
) extends Splitter[Unit] {

    private var source: DataFrame = null
    private var path: String = null
    private var hdfs: PartitionTreeHDFSInterface = null

    override def prepare(df: DataFrame, path: String) = {
      this.path = path
      source = df
      hdfs = PartitionTreeHDFSInterface(df.sparkSession, path)
      this
    }

    def isPrepared: Boolean = path != null
    
    override def getPath = { assertPrepared; path }

    protected def getHDFS = { assertPrepared; hdfs }

    private var doFinalize = true;

    override def doFinalize(enabled: Boolean) = { doFinalize = enabled; this }

    override def finalizeEnabled = doFinalize

    override def sortWith(sorter: Sorter) = { this.sorter = sorter; this }

    override protected def load(context: Unit): DataFrame = source

    /**
      * A Grouper to group the data by. Each grouping represents one kind of data
      * This grouper is used for splitting the data
      */
    protected def splitGrouper: Grouper

    /**
      * Build buckets based off the grouped data
      *
      * @param df the data groups grouped by [[splitGrouper]]
      * @return the buckets of data represented as sets of data groupings
      */
    protected def split(df: DataFrame): Seq[DataFrame]

    /**
      * Build the [[PartitionTree]] from the given buckets. The list of buckets
      * is ordered in the same order as they were returned by [[split]] and are
      * thus also grouped by [[sortGrouper]]
      *
      * @param buckets the buckets
      */
    protected def buildTree(buckets: Seq[PartitionFolder]): PartitionTree[String]

    /**
      * Write the given tree's schema to the file system
      *
      * @param tree the partition tree to write
      * @param spark the spark session used to write it
      */
    protected def writeMetadata(tree: PartitionTree[String]): Unit
      = getHDFS.write(tree) 

    override def partition(): Unit = {
        assertPrepared
        
        val types = data.group(splitGrouper)
        types.groups.persist()
        try {
          val sorted = for (bucket <- split(types.groups)) yield
            types.copy(groups = bucket)
              .group(sorter.grouper)
              .transform(sorter.sort)
              .transform{
                case IntermediateData(groups, grouper, source) => IntermediateData.fromRaw(grouper.sort(groups, source))
              }.toDF
              .transform(finalize)
          sorted |> write |> buildTree |> writeMetadata
        } finally types.groups.unpersist()
    }

    protected def finalize(bucket: DataFrame): DataFrame
      = if (doFinalize) bucket.repartition(1) else bucket

    protected def write(buckets: Seq[DataFrame]): Seq[PartitionFolder]
      = buckets match {
        // if we have just one bucket, we can forgo a lot of unnecessary steps
        case head :: Nil => Seq(writeOne(head))
        case _ => writeMany(buckets)
      }

    protected def writeOne(bucket: DataFrame): PartitionFolder = {
      val targetFolder = PartitionFolder.makeFolder(path, false)
      bucket.write.parquet(targetFolder.filename)
      targetFolder
    }

    protected def writeMany(buckets: Seq[DataFrame]): Seq[PartitionFolder]
      = buckets.par.map(writeOne(_)).seq

    protected def data: IntermediateData = IntermediateData.fromRaw(data(()))
}
