package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode

/**
  * Set the Shape to a predefined value.
  *
  * @param shape
  */
final case class Predefined(
  shape: AnyNode[Unit]
) extends PipelineStep {

  override def supports(state: PipelineState): Boolean = true

  override def run(state: PipelineState): PipelineState
    = Shape(state) = shape

}
