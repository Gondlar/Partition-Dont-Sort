package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.Spill

/**
  * This PipelineStep splits the input data into buckets of even size (as far as
  * the size of the dataset is divisible by the requested number of partitions)
  *
  * @param numPartitions The requested number of partitions. The number must be
  *   strictly positive.
  */
final case class ParallelEvenBuckets(numPartitions: Int) extends PipelineStep {

  require(numPartitions > 0)

  override def supports(state: PipelineState): Boolean
    = !ModifySchema(state)

  override def run(state: PipelineState): PipelineState = {
    val newData = state.data.repartition(numPartitions)
    Shape(state.copy(data = newData)) = makeShape
  }

  private def makeShape = {
    val buckets = Iterator.fill(numPartitions-1)(Bucket(()))
    buckets.foldLeft(Bucket(()): AnyNode[Unit])((partitioned,bucket) => Spill(partitioned,bucket))
  }
}
