package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline._

final case class LocalOrder(
  sorter: ColumnOrderer
) extends PipelineStep {

  override def isSupported(state: PipelineState): Boolean
    = Buckets.isDefined(state) && sorter.isSupported(state)

  override def run(state: PipelineState): PipelineState = {
    BucketSortorders(state) = Buckets(state).map({bucket => sorter.sort(state, bucket)})
  }

}
