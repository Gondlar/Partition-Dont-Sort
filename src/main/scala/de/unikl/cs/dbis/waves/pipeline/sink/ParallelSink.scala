package de.unikl.cs.dbis.waves.pipeline.sink

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.spark_partition_id
import de.unikl.cs.dbis.waves.util.operators.findBucket
import java.util.UUID

/**
  * Write partitions to disk in parallel using sparks partitionBy writer
  * The result is always finalized but must be prepared such that buckets are
  * not split between partitions.
  * @see [[ShuffleByShape]]
  */
class ParallelSink(
  partitioner: ParallelPartitioner
) extends PipelineSink with AlwaysFinalized {

  override def supports(state: PipelineState): Boolean
    = (partitioner supports state) && (!ModifySchema(state))

  override def run(state: PipelineState): (PipelineState, Seq[PartitionFolder]) = {
    // write partitioned
    val (names, partitionerColumn) = partitioner(state)
    state.data.withColumn(BUCKET_COLUMN, partitionerColumn)
      .write
      .partitionBy(BUCKET_COLUMN)
      .parquet(state.path)

    // Rename all to prevent future name collisions
    implicit val fs = state.hdfs.fs
    val folders = for (index <- names) yield {
      val folder = new PartitionFolder(state.path, s"$BUCKET_COLUMN=${index}", false)
      if (folder.exists) {
        folder.mv(newName = UUID.randomUUID().toString())
        folder
      } else {
        val emptyFolder = PartitionFolder.makeFolder(state.path, false)
        emptyFolder.mkdir
        emptyFolder
      }
    }
    (state, folders.toSeq)
  }

  private val BUCKET_COLUMN = "___BUCKET_COLUMN___"
}

trait ParallelPartitioner {

  /**
    * Get the bucket values as well as a Spark Column to distinguish between them
    *
    * @param state the current state
    * @return The second return value is a Spark Colum returning the colum used
    *         for assigning buckets. The first is an Iterable containing the
    *         possible values in the order in which they correspond to Buckets
    *         in the tree
    */
  def apply(state: PipelineState): (Iterable[_], Column)

  /**
    * Check whether this Partitioner is supported in the given state
    *
    * @param state the current pipeline state
    * @return true iff this Partitioner can be run in that state
    */
  def supports(state: PipelineState): Boolean
}

object ParallelSink {

  /**
    * This flavor of the [[ParallelSink]] creates buckets based on the shape
    */
  val byShape = new ParallelSink(new ParallelPartitioner {

    override def apply(state: PipelineState): (Iterable[_], Column) = {
      val shape = Shape(state)
      val indexes = shape.indexes
      (shape.indexes.buckets.map(_.data), findBucket(indexes, Schema(state)))
    }

    override def supports(state: PipelineState): Boolean
      = Shape isDefinedIn state
  })

  /**
    * This flavor of the [[ParallelSink]] creates buckets based on the partitions
    */
  val byPartition = new ParallelSink( new ParallelPartitioner {

    override def apply(state: PipelineState): (Iterable[_], Column)
      = (0 until state.data.rdd.getNumPartitions, spark_partition_id)

    override def supports(state: PipelineState): Boolean = true
  })
}
