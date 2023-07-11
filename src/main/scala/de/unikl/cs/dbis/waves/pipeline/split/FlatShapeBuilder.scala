package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode

/**
  * This pipeline step Generates a flat shape from a list of buckets.
  * The result is a degenerated tree which implements a list using Spill nodes.
  * This is intended for splits which carry no semantic information on the split.
  */
object FlatShapeBuilder extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = Buckets.isDefined(state)

  override def run(state: PipelineState): PipelineState =  {
    val buckets = Iterator.fill(Buckets(state).length-1)(Bucket(()))
    val tree = buckets.foldLeft(Bucket(()): AnyNode[Unit])((partitioned,bucket) => Spill(partitioned,bucket))
    Shape(state) = tree
  }
}
