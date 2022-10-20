package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.PriorityQueue

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, Bucket, SplitByPresence}
import de.unikl.cs.dbis.waves.partitions.{SplitByPresencePath, Absent, Present}
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.split.recursive.{Heuristic, EvenHeuristic, GroupedCalculator}
import de.unikl.cs.dbis.waves.util.{Logger, PartitionFolder}
import de.unikl.cs.dbis.waves.util.operators.{Grouper, PresenceGrouper}

import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode

class HeuristicSplitter(
  threshold: Long,
  heuristic: Heuristic = EvenHeuristic
) extends GroupedSplitter {
  private implicit val ord = Ordering.by[Seq[SplitByPresencePath], Int](_.size)

  private var partitions: AnyNode[DataFrame] = null
  private var queue: PriorityQueue[(Long,Seq[SplitByPresencePath])] = null

  private val SPLIT_GROUPER = PresenceGrouper

  override def prepare(df: DataFrame, path: String) = {
    super.prepare(df, path)
    queue = PriorityQueue((Long.MaxValue, Seq.empty[SplitByPresencePath]))
    this
  }

  override protected def splitGrouper: Grouper = SPLIT_GROUPER

  override protected def split(df: DataFrame): (Seq[DataFrame], Seq[PartitionMetadata]) = {
    Logger.log("evenSplitter-start")
    val calc = GroupedCalculator(data.sourceSchema)
    val pathMap = calc.paths(df).zipWithIndex.toMap

    partitions = Bucket(df)
    while (queue.nonEmpty) {
      val (count, pathToNext) = queue.dequeue()
      Logger.log("evenSplitter-start-partition", pathToNext)
      val next = partitions.find(pathToNext).get.asInstanceOf[Bucket[DataFrame]]
      val metadata = partitions.metadataFor(pathToNext)
      val nextData = next.data
      val nextSplit = heuristic.choose(calc, nextData, metadata, threshold.toDouble/count)
      Logger.log("evenSplitter-choseSplit", nextSplit)
      nextSplit match {
        case None => Logger.log("evenSplitter-noGoodSplitFound")
        case Some(path) => {
          val pathIndex = pathMap(path)
          val present = addPartition(nextData, pathIndex, pathToNext :+ Present)
          val absent = addPartition(nextData, pathIndex, pathToNext :+ Absent)
          assert(present.intersect(absent).count() == 0)
          val newSplit = SplitByPresence(path, Bucket(present), Bucket(absent))
          partitions = partitions.replace(pathToNext, newSplit)
        }
      }
      Logger.log("evenSplitter-end-partition", pathToNext)
    }
    (partitions.buckets.map(_.data).toSeq, partitions.metadata())
  }

  private def addPartition(df: DataFrame, index : Int, location: Seq[SplitByPresencePath]) = {
    val filterColumn = SPLIT_GROUPER.GROUP_COLUMN(index) === (location.last match {
      case Absent => false
      case Present => true
    })
    val newPartiton = df.filter(filterColumn)
    val size = SPLIT_GROUPER.count(newPartiton)
    if (size > threshold) queue.enqueue((size, location))
    newPartiton
  }

  override protected def buildTree(folders: Seq[PartitionFolder]): PartitionTree[String]
    = new PartitionTree(data.sourceSchema, sorter, partitions.map((_, index) => folders(index).name))
}
