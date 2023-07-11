package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline.PipelineStep
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.pipeline.GlobalSortorder

/**
  * Uses the provided ColumnOrderer to derive a Global Sortorder.
  *
  * @param sorter A ColumnOrderer which is supported in the state
  */
final case class GlobalOrder(
  sorter: ColumnOrderer
) extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = sorter supports state

  override def run(state: PipelineState): PipelineState
    = GlobalSortorder(state) = sorter.sort(state, state.data)

}
