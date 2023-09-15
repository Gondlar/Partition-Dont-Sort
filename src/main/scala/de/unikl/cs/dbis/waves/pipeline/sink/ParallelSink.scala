package de.unikl.cs.dbis.waves.pipeline.sink

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.findBucket
import java.util.UUID

/**
  * Write partitions to disk by reading the Shape from the state and writing
  * the data partitioned by buckets
  */
object ParallelSink extends PipelineSink {

  override def supports(state: PipelineState): Boolean
    = (Shape isDefinedIn state) && (!ModifySchema(state))

  override def run(state: PipelineState): (PipelineState, Seq[PartitionFolder]) = {
    // write partitioned
    val shape = Shape(state)
    val indexes = shape.indexes
    state.data.withColumn(BUCKET_COLUMN, findBucket(indexes, Schema(state)))
      .write
      .partitionBy(BUCKET_COLUMN)
      .parquet(state.path)

    // Rename all to prevent future name collisions
    implicit val fs = state.hdfs.fs
    val folders = for (index <- indexes.buckets) yield {
      val folder = new PartitionFolder(state.path, s"$BUCKET_COLUMN=${index.data}", false)
      if (folder.exists) {
        folder.mv(newName = UUID.randomUUID().toString())
        folder
      } else {
        val emptyFolder = PartitionFolder.makeFolder(state.path, false)
        emptyFolder.mkdir
        emptyFolder
      }
    }
    (state, folders)
  }

  private val BUCKET_COLUMN = "___BUCKET_COLUMN___"

}
