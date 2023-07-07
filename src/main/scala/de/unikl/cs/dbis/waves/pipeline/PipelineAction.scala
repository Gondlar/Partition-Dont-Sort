package de.unikl.cs.dbis.waves.pipeline

import de.unikl.cs.dbis.waves.util.PartitionFolder

trait PipelineAction[T] {
  final def apply(state: PipelineState) : T = {
    require(isSupported(state))
    run(state)
  }

  def isSupported(state: PipelineState): Boolean
  def run(state: PipelineState): T
}

trait PipelineStep extends PipelineAction[PipelineState]
trait PipelineSink extends PipelineAction[Seq[PartitionFolder]]