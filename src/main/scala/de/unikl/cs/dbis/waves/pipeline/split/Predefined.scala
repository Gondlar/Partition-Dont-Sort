package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

/**
  * Set the Shape to a predefined value.
  *
  * @param shape
  */
final case class Predefined(
  shape: AnyNode[Unit]
) extends PipelineStep with NoPrerequisites {

  override def run(state: PipelineState): PipelineState = {
    val newState = Shape(state) = shape
    NumBuckets(newState) = shape.buckets.size
  }

}
