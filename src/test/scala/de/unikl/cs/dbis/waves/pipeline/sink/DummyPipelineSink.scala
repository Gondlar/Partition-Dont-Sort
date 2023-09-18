package de.unikl.cs.dbis.waves.pipeline.sink

import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.pipeline._

final case class DummyPipelineSink(
  supported: Boolean,
  result: Seq[PartitionFolder] = Seq.empty,
  resultState: Option[PipelineState] = None,
  isFinalized: Boolean = false
) extends PipelineSink {
  override def supports(state: PipelineState): Boolean = supported
  override def isAlwaysFinalizedFor(state: PipelineState): Boolean = isFinalized
  override def run(state: PipelineState) = (resultState.getOrElse(state), result)
}
