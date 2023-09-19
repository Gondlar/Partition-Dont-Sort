package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline._

/**
  * Sort the partitions of a Pipeline lexicographically using the global
  * sortorder
  */
object ParallelSorter extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = (GlobalSortorder isDefinedIn state)

  override def run(state: PipelineState): PipelineState
    = state.copy(data = state.data.sortWithinPartitions(GlobalSortorder(state):_*))
}
