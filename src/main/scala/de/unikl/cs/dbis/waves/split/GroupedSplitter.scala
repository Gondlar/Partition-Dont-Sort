package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.{SparkSession, DataFrame}
import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.operators.Grouper
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
abstract class GroupedSplitter extends Splitter[Unit] {

    private var source: DataFrame = null
    private var path: String = null

    override def prepare(df: DataFrame, path: String) = {
      this.path = path
      source = df
      this
    }

    def isPrepared: Boolean = path != null
    
    override def getPath = { assertPrepared; path }

    override protected def load(context: Unit): DataFrame = source

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
    protected def buildTree(buckets: Seq[PartitionFolder]): PartitionTree[String]

    /**
      * Write the given tree's schema to the file system
      *
      * @param tree the partition tree to write
      * @param spark the spark session used to write it
      */
    protected def writeMetadata(tree: PartitionTree[String], spark: SparkSession): Unit
      = PartitionTreeHDFSInterface(spark, path).write(tree) 

    override def partition(): Unit = {
        assertPrepared
        
        val types = splitGrouper.group(data)
        types.persist()
        try {
          val buckets = split(types)
          val sorted = buckets.map{ b => 
              sort(sortGrouper.from(splitGrouper, b, data))
          }
          val folders = write(sorted, data)
          writeMetadata(buildTree(folders), data.sparkSession)
        } finally types.unpersist()
    }

    protected def write(buckets: Seq[DataFrame], rawData: DataFrame): Seq[PartitionFolder]
      = buckets match {
        // if we have just one bucket, we can forgo a lot of unnecessary steps
        case head :: Nil => Seq(writeOne(head, rawData))
        case _ => writeMany(buckets, rawData)
      }

    protected def writeOne(bucket: DataFrame, data: DataFrame): PartitionFolder = {
      val sorted = sortGrouper.sort(bucket, data)
      val targetFolder = PartitionFolder.makeFolder(path, false)
      sorted.write.parquet(targetFolder.filename)
      targetFolder
    }

    protected def writeMany(buckets: Seq[DataFrame], rawData: DataFrame): Seq[PartitionFolder] = {
      val spark = rawData.sparkSession

      val tempPath = new Path(path + PartitionFolder.TEMP_DIR)
      sortGrouper.matchAll(buckets, rawData)
                 .write
                 .partitionBy(sortGrouper.matchColumn)
                 .parquet(tempPath.toString())

      implicit val ec: ExecutionContext = ExecutionContext.global
      val futureFolders = for ((bucket, bucketId) <- buckets.zipWithIndex) yield {
        Future {
          val intermediaryFolder = new PartitionFolder(path, s"${sortGrouper.matchColumn}=$bucketId", true)
          val data = spark.read
                          .parquet(intermediaryFolder.filename)
                          .drop(sortGrouper.matchColumn.col)
          val finalFolder = blocking { writeOne(bucket, data) }
          intermediaryFolder.delete(intermediaryFolder.file.getFileSystem(spark.sparkContext.hadoopConfiguration))
          finalFolder
        }
      }
      val result = Await.result(Future.sequence(futureFolders), Duration.Inf)
      tempPath.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(tempPath, true)
      result
    }

    protected def data: DataFrame = data(())
}
