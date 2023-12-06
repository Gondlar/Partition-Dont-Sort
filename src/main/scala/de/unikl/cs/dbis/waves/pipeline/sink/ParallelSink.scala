package de.unikl.cs.dbis.waves.pipeline.sink

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

import java.util.UUID

/**
  * Write partitions to disk in parallel using sparks partitionBy writer
  * The result is always finalized but must be prepared such that buckets are
  * not split between partitions.
  * @see [[ShuffleByShape]]
  */
object ParallelSink extends PipelineSink with AlwaysFinalized {

  override def supports(state: PipelineState): Boolean
    = (NumBuckets isDefinedIn state) && (ShuffleColumn isDefinedIn state) && (!ModifySchema(state))

  override def run(state: PipelineState): (PipelineState, Seq[PartitionFolder]) = {
    // write partitioned
    val names = 0 until NumBuckets(state)
    val shuffleColumn = ShuffleColumn(state)
    state.data
      .write
      .partitionBy(shuffleColumn)
      .parquet(state.path)

    // Rename all to prevent future name collisions
    implicit val fs = state.hdfs.fs
    val folders = for (index <- names) yield {
      val folder = new PartitionFolder(state.path, s"$shuffleColumn=${index}", false)
      if (folder.exists) {
        folder.mv(newName = UUID.randomUUID().toString())
        folder
      } else {
        val emptyFolder = PartitionFolder.makeFolder(state.path, false)
        emptyFolder.mkdir
        emptyFolder
      }
    }
    val cleanDf = state.data.drop(shuffleColumn)
    (state.copy(data = cleanDf), folders.toSeq)
  }
}
