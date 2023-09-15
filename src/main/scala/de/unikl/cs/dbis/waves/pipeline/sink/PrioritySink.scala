package de.unikl.cs.dbis.waves.pipeline.sink

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder

/**
  * Pick a usable sink from a list of given sinks. Sinks which appear earlier in
  * the list are prefferred over sinks later in the list if both are supported,
  * @param sinks the sinks
  */
final case class PrioritySink(
  sinks: PipelineSink*
) extends PipelineSink {

  override def supports(state: PipelineState): Boolean
    = sinks.exists(_ supports state)

  override def run(state: PipelineState): (PipelineState, Seq[PartitionFolder])
    = sinks.find(_ supports state).get.run(state)

}
