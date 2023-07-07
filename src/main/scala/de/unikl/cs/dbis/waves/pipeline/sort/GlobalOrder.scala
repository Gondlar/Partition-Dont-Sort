package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline.PipelineStep
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.pipeline.GlobalSortorder

final case class GlobalOrder(
  sorter: ColumnOrderer
) extends PipelineStep {

  override def isSupported(state: PipelineState): Boolean
    = sorter.isSupported(state)

  override def run(state: PipelineState): PipelineState
    = GlobalSortorder(state) = sorter.sort(state, state.data)

}
