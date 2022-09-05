package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{col,sum,map_from_entries,monotonically_increasing_id,struct,typedLit,collect_list}
import org.apache.spark.sql.types.StructType

import scala.concurrent.{Future, ExecutionContext, blocking, Await}
import scala.concurrent.duration.Duration
import scala.collection.mutable.PriorityQueue

import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.partitions.{PartitionTree, Bucket, SplitByPresence, PartitionTreeHDFSInterface}
import de.unikl.cs.dbis.waves.partitions.{PartitionTreePath, Absent, Present}
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.operators.presence
import de.unikl.cs.dbis.waves.split.recursive.{EvenHeuristic, GroupedCalculator}

class EvenSplitter(input: DataFrame, threshold: Long, path: String) extends GroupedSplitter {
  private implicit val ord = Ordering.by[Seq[PartitionTreePath], Int](_.size)

  private val partitions  = new PartitionTree(input.schema, Bucket(input))
  private val queue = PriorityQueue((Long.MaxValue, Seq.empty[PartitionTreePath]))

  override protected def load(context: Unit): DataFrame = input

  override protected def grouper: StructType => Column = presence

  override protected def split(df: DataFrame): Seq[DataFrame] = {
    val heuristic = EvenHeuristic()
    val calc = GroupedCalculator(data.schema)
    val pathMap = calc.paths(df).zipWithIndex.toMap

    partitions.replace(partitions.root, Bucket(df))
    while (queue.nonEmpty) {
      val (count, pathToNext) = queue.dequeue()
      val next = partitions.find(pathToNext).get.asInstanceOf[Bucket[DataFrame]]
      val nextData = next.data
      val nextSplit = heuristic.choose(calc, df, Seq.empty, Seq.empty, threshold.toDouble/count)
      // evenHeuristic(determinePresenceFromGroups(nextData), partitions.globalSchema, )
      nextSplit match {
        case None => {} // No good split found, stop
        case Some(path) => {
          val pathIndex = pathMap(path)
          val present = addPartition(nextData, pathIndex, pathToNext :+ Present)
          val absent = addPartition(nextData, pathIndex, pathToNext :+ Absent)
          val newSplit = SplitByPresence(path, Bucket(present), Bucket(absent))
          partitions.replace(next, newSplit) //TODO: urgh... we have the path and still traverse the entire tree
        }
      }
    }
    partitions.getBuckets().map(_.data).toSeq
  }

  private def addPartition(df: DataFrame, index : Int, location: Seq[PartitionTreePath]) = {
    val filterColumn = col("presence")(index) === (location.last match {
      case Absent => false
      case Present => true
      case _ => assert(false)
    })
    val newPartiton = df.filter(filterColumn)
    val size = newPartiton.agg(sum(col("count"))).collect().head.getLong(0)
    if (size > threshold) queue.enqueue((size, location))
    newPartiton.sort()
  }

  override protected def buildTree(buckets: Seq[DataFrame]): Unit = {
    val df = data
    val dataColumns = df.columns.map(col(_))
    val group = grouper(df.schema)
    
    val partition_index_column = "partition_index-obBnTMc1tD2ujPb0uJLO"
    val allBucketsWithIndex = buckets.zipWithIndex.map { case (bucket, index) =>
      bucket.select(bucket.col("presence"), typedLit(index).as(partition_index_column))
    }.reduce{(lhs, rhs) =>
      lhs.unionAll(rhs)
    }
    
    val grouping_col = "group-WiFJH26knAV3jdiUnTQy"
    val map_column = "map-tUIsVUPnFCovRqfjhyo4"
    df.crossJoin(dataFrameToMap(allBucketsWithIndex, col("presence"), col(partition_index_column), map_column))
      .withColumn(grouping_col, col(map_column)(group))
      .select((dataColumns :+ col(grouping_col)):_*)
      .write
      .partitionBy(grouping_col)
      .parquet(path)

    implicit val ec: ExecutionContext = ExecutionContext.global
    val futureFolders = for ((bucket, bucketId) <- buckets.zipWithIndex) yield {
      Future {
        val order_name = "order-gdcKu5JfdNK8y8Re01N5"
        val map_column = "map-d03fZcpv2dAd9ydKBZG8"
        val sort_column = "sort-4Jqq6ueeMXyjTTKTnwsG"

        val intermediaryFolder = new PartitionFolder(path, s"$grouping_col=$bucketId", false)
        val finalFolder = PartitionFolder.makeFolder(path, false)

        val preppedBucket = bucket.withColumn(order_name, monotonically_increasing_id)
        val sorted = df.sparkSession
                       .read
                       .parquet(intermediaryFolder.filename)
                       // Store sort order in a map and join that map in the df
                       .crossJoin(dataFrameToMap(preppedBucket, col("presence"), col(order_name), map_column))
                       // Determine sort order
                       .withColumn(sort_column, col(map_column)(group))
                       // Repartition and sort
                       .repartition(1)
                       .sort(col(sort_column))
                       // Remove all intermediary data
                       .select(dataColumns:_*)
        blocking { sorted.write.parquet(finalFolder.filename) }
        intermediaryFolder.delete(intermediaryFolder.file.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration))
        finalFolder
      }
    }
    val folders = Await.result(Future.sequence(futureFolders), Duration.Inf)
    val tree = partitions.map((_, index) => folders(index).name)
    PartitionTreeHDFSInterface(df.sparkSession, path).write(tree)
  }

  private def dataFrameToMap(df: DataFrame, key: Column, value: Column, map_column: String) = {
    assert(key.expr.deterministic)
    assert(value.expr.deterministic)
    val struct_list_column = "structs-ib0aWRdVFIfdRV6KDasq"
    df.agg(collect_list(struct(key, value)).as(struct_list_column))
      .select(map_from_entries(col(struct_list_column)).as(map_column))
  }
}
