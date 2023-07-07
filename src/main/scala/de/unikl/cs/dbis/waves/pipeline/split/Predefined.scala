package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode

final case class Predefined(
  shape: AnyNode[Unit]
) extends PipelineStep {

  override def isSupported(state: PipelineState): Boolean = true

  override def run(state: PipelineState): PipelineState
    = Shape(state) = shape

}
