package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline.PipelineStep
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.util.operators.TempColumn

import org.apache.spark.sql.functions.monotonically_increasing_id
import de.unikl.cs.dbis.waves.pipeline.Buckets
import de.unikl.cs.dbis.waves.pipeline.NoPrerequisites

/**
  * This PipelineStep splits the input data into buckets of even size (as far as
  * the size of the dataset is divisible by the requested number of partitions)
  * 
  * This step does not set Shape, consider running [[FlatShapeBuilder]] afterwards.
  *
  * @param numPartitions The requested number of partitions. The number must be
  *   strictly positive. While 1 is supported, consider using [[SingleBucket]]
  *   instead.
  */
final case class EvenBuckets(numPartitions: Int) extends PipelineStep with NoPrerequisites {

  require(numPartitions > 0)

  override def run(state: PipelineState): PipelineState = {
    val tmp = TempColumn.apply("part")
    val withPartitionId = state.data.withColumn(tmp, monotonically_increasing_id() mod numPartitions)
    val buckets = 0.until(numPartitions)
      .map(n => withPartitionId.where(tmp === n).drop(tmp.col))
    Buckets(state) = buckets
  }
}
