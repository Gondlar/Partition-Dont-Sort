package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.Logger

/**
  * Pick a usable step from a list of given steps. Steps which appear earlier in
  * the list are preferred over steps later in the list if both are supported,
  * @param steps the steps
  */
final case class PriorityStep(
  steps: PipelineStep*
) extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = steps.exists(_ supports state)

  override def run(state: PipelineState): PipelineState = {
    val chosen = steps.find(_ supports state).get
    Logger.log("step-chosen", chosen.name)
    chosen.run(state)
  }
}
