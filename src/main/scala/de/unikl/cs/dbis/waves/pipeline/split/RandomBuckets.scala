package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline.PipelineStep
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.util.operators.TempColumn

import org.apache.spark.sql.functions.monotonically_increasing_id
import de.unikl.cs.dbis.waves.pipeline.Buckets

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
final case class RandomBuckets(numPartitions: Int) extends PipelineStep {

  require(numPartitions > 0)

  override def supports(state: PipelineState): Boolean = true

  override def run(state: PipelineState): PipelineState = {
    val buckets = state.data.randomSplit(Array.fill(numPartitions)(1)).filter(!_.isEmpty)
    Buckets(state) = buckets
  }
}
