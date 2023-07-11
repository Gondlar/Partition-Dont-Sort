package de.unikl.cs.dbis.waves.pipeline.sink

import de.unikl.cs.dbis.waves.pipeline._
import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.PartitionFolder

/**
  * Write partitions to disk by reading the Buckets array from the state and
  * creating one PartitionFolder for each one.
  */
object DataframeSink extends PipelineSink {

  override def supports(state: PipelineState): Boolean
    = Buckets isDefinedIn state

  override def run(state: PipelineState): Seq[PartitionFolder] = {
    val buckets = Buckets(state)
    buckets match {
      case head :: Nil => Seq(writeOne(head, state.path))
      case _ => writeMany(buckets, state.path)
    }
  }

  protected def writeOne(bucket: DataFrame, path: String): PartitionFolder = {
    //TODO handle empty df?
    val targetFolder = PartitionFolder.makeFolder(path, false)
    bucket.write.parquet(targetFolder.filename)
    targetFolder
  }

  protected def writeMany(buckets: Seq[DataFrame], path: String): Seq[PartitionFolder]
    = buckets.par.map(writeOne(_, path)).seq

}
