package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.concurrent.{Future, ExecutionContext, blocking, Await}
import scala.concurrent.duration.Duration
import scala.collection.mutable.PriorityQueue

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, Bucket, SplitByPresence, PartitionTreeHDFSInterface}
import de.unikl.cs.dbis.waves.partitions.{PartitionTreePath, Absent, Present}
import de.unikl.cs.dbis.waves.split.recursive.{EvenHeuristic, GroupedCalculator}
import de.unikl.cs.dbis.waves.util.{Logger, PartitionFolder}
import de.unikl.cs.dbis.waves.util.operators.{Grouper, PresenceGrouper}

class EvenSplitter(input: DataFrame, threshold: Long, path: String) extends GroupedSplitter {
  private implicit val ord = Ordering.by[Seq[PartitionTreePath], Int](_.size)

  private val partitions  = new PartitionTree(input.schema, Bucket(input))
  private val queue = PriorityQueue((Long.MaxValue, Seq.empty[PartitionTreePath]))

  override protected def load(context: Unit): DataFrame = input

  private val SPLIT_GROUPER = PresenceGrouper

  override protected def splitGrouper: Grouper = SPLIT_GROUPER

  override protected def split(df: DataFrame): Seq[DataFrame] = {
    Logger.log("evenSplitter-start")
    val heuristic = EvenHeuristic()
    val calc = GroupedCalculator(data.schema)
    val pathMap = calc.paths(df).zipWithIndex.toMap

    partitions.replace(partitions.root, Bucket(df))
    while (queue.nonEmpty) {
      val (count, pathToNext) = queue.dequeue()
      Logger.log("evenSplitter-start-partition", pathToNext)
      val next = partitions.find(pathToNext).get.asInstanceOf[Bucket[DataFrame]]
      val (absent, present) = partitions.knownAbsentAndPresentIn(pathToNext)
      val nextData = next.data
      val nextSplit = heuristic.choose(calc, nextData, absent, present, threshold.toDouble/count)
      Logger.log("evenSplitter-choseSplit", nextSplit)
      nextSplit match {
        case None => Logger.log("evenSplitter-noGoodSplitFound")
        case Some(path) => {
          val pathIndex = pathMap(path)
          val present = addPartition(nextData, pathIndex, pathToNext :+ Present)
          val absent = addPartition(nextData, pathIndex, pathToNext :+ Absent)
          assert(present.intersect(absent).count() == 0)
          val newSplit = SplitByPresence(path, Bucket(present), Bucket(absent))
          partitions.replace(next, newSplit) //TODO: urgh... we have the path and still traverse the entire tree
        }
      }
      Logger.log("evenSplitter-end-partition", pathToNext)
    }
    partitions.getBuckets().map(_.data).toSeq
  }

  private def addPartition(df: DataFrame, index : Int, location: Seq[PartitionTreePath]) = {
    val filterColumn = SPLIT_GROUPER.GROUP_COLUMN(index) === (location.last match {
      case Absent => false
      case Present => true
      case _ => assert(false)
    })
    val newPartiton = df.filter(filterColumn)
    val size = SPLIT_GROUPER.count(newPartiton)
    if (size > threshold) queue.enqueue((size, location))
    newPartiton
  }

  override protected def buildTree(buckets: Seq[DataFrame]): Unit = {
    val df = data
    val spark = df.sparkSession
    
    Logger.log("evenSplitter-start-buildTree")
    sortGrouper.matchAll(buckets, df)
               .write
               .partitionBy(sortGrouper.matchColumn)
               .parquet(path)

    Logger.log("evenSplitter-grouping-done")

    implicit val ec: ExecutionContext = ExecutionContext.global
    val futureFolders = for ((bucket, bucketId) <- buckets.zipWithIndex) yield {
      Future {
        val intermediaryFolder = new PartitionFolder(path, s"${sortGrouper.matchColumn}=$bucketId", false)
        val finalFolder = PartitionFolder.makeFolder(path, false)
        val partition = spark.read
                             .parquet(intermediaryFolder.filename)
                             .drop(sortGrouper.matchColumn.col)
        val sorted = sortGrouper.sort(bucket, partition)
        blocking { sorted.write.parquet(finalFolder.filename) }
        intermediaryFolder.delete(intermediaryFolder.file.getFileSystem(spark.sparkContext.hadoopConfiguration))
        finalFolder
      }
    }
    val folders = Await.result(Future.sequence(futureFolders), Duration.Inf)
    val tree = partitions.map((_, index) => folders(index).name)
    PartitionTreeHDFSInterface(spark, path).write(tree)
    Logger.log("evenSplitter-end-buildTree")
  }
}
