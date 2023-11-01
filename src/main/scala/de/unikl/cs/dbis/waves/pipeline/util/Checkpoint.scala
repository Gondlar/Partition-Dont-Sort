package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._

/**
  * This pipeline step creates a Checkpoint of the DataFrame in the step
  * 
  * This can speed up the process if the calculation up to this point is already
  * very expensive. Empirically, JSON parsing alone is just not expensive enough.
  */
object Checkpoint extends PipelineStep with NoPrerequisites {

  override def run(state: PipelineState): PipelineState = {
    state.data.sparkSession.sparkContext.setCheckpointDir(state.path + "_checkpoints")
    state.copy(data = state.data.checkpoint())
  }
}
