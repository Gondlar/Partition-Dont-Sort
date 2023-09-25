package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.operators.TempColumn

/**
  * This PipelineStep splits the input data into buckets of approximately even
  * size using random assignment. The result may have fewer buckets than requested,
  * especially for very small datasets.
  * 
  * This step does not set Shape, consider running [[FlatShapeBuilder]] afterwards.
  *
  * @param numPartitions The requested number of partitions. The number must be
  *   strictly positive. While 1 is supported, consider using [[SingleBucket]]
  *   instead.
  */
final case class RandomBuckets(numPartitions: Int) extends PipelineStep with NoPrerequisites {

  require(numPartitions > 0)

  override def run(state: PipelineState): PipelineState = {
    val buckets = state.data.randomSplit(Array.fill(numPartitions)(1)).filter(!_.isEmpty)
    val newState = Buckets(state) = buckets
    NumBuckets(newState) = numPartitions
  }
}
