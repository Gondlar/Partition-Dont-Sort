package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.{SparkSession, DataFrame}
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
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

import Transform._

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

    protected var schemaModificationsEnabled = false

    override def modifySchema(enabled: Boolean)
      = { schemaModificationsEnabled = enabled; this }

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
      * @return the buckets of data represented as sets of data groupings as
      *         well as the metadata known for each bucket. The metadata is used
      *         to optimize the written schema if schema modification are
      *         enabled
      */
    protected def split(df: DataFrame): (Seq[DataFrame], Seq[PartitionMetadata])

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
          val (buckets, metadata) = split(types.groups)
          assert(buckets.size == metadata.size)
          val sorted = for ((bucket, metadata) <- buckets.zip(metadata)) yield
            types.copy(groups = bucket)
              .group(sorter.grouper)
              .transform(sorter.sort)
              .transform{
                case IntermediateData(groups, grouper, source) => IntermediateData.fromRaw(grouper.sort(groups, source))
              }.toDF
	            .transform(conditionally(schemaModificationsEnabled, clipSchema(metadata)))
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

object Transform {

  /**
    * Modify the schema of the given DataFrame according to the given metadata.
    * Columns listed as absent in the metadata are removed. Columns listed as
    * present in the metadata are marked as such in the schema.
    * 
    * It is not checked whether the data in the given DataFrame conforms to
    * the given metadata. If the metadata lists a column as absent which still
    * contains data in df, that data is lost. If the metadata lists a column as
    * present even though df has absent values for that column, reading from the
    * returned DataFrame will fail.
    *
    * @param metadata the medatata
    * @param df the DataFrame to transform
    * @return the transformed DataFrame
    */
  def clipSchema(metadata: PartitionMetadata)(df: DataFrame) = {
    import de.unikl.cs.dbis.waves.util.nested.schemas._

    val dropped = metadata.getAbsent.foldLeft(df)((df, path) => df.drop(path.toSpark))
    val schema = metadata.getPresent.foldLeft(dropped.schema)((schema, path) => schema.withPresent(path))
    dropped.sparkSession.createDataFrame(dropped.rdd, schema)
  }

  /**
    * Apply a transform only if a condition holds
    *
    * @param condition the condition
    * @param transform the transform
    * @param df the DataFrame to transform
    * @return if condition was true, df with transform applied to it,
    *         otherwise just df unchanged
    */
  def conditionally(condition: Boolean, transform: DataFrame => DataFrame)(df: DataFrame)
    = if (condition) df.transform(transform) else df
}
