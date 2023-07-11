package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline._

/**
  * Use the given ColumnOrderer to derive Sort Orders for each Bucket.
  *
  * @param sorter A ColumnOrderer which is supported in the state
  */
final case class LocalOrder(
  sorter: ColumnOrderer
) extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = Buckets.isDefined(state) && (sorter supports state)

  override def run(state: PipelineState): PipelineState = {
    BucketSortorders(state) = Buckets(state).map({bucket => sorter.sort(state, bucket)})
  }

}
