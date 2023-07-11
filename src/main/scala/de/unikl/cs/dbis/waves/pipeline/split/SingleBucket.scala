package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode

/**
  * This pipeline step moves all data into one partition by creating a shape
  * which consists of a single bucket and a corresponding Buckets list which
  * just contains the input DataFrame.
  */
object SingleBucket extends PipelineStep with NoPrerequisites {

  override def run(state: PipelineState): PipelineState
    = Buckets(Shape(state) = Bucket(())) = Seq(state.data)
}
