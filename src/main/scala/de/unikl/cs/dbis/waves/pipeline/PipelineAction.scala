package de.unikl.cs.dbis.waves.pipeline

import de.unikl.cs.dbis.waves.util.PartitionFolder

/**
  * Abstract base class for all operations in a Pipeline.
  * 
  * The type parameter is the action's result type.
  */
trait PipelineAction[T] {

  /**
    * Perform the step on the given state.
    * 
    * First, check if the state is supported by the action. If not, the action
    * has no side effects and throws an exception. If the state is supported,
    * the action is run and its result returned
    *
    * @param state the state when the action is run
    * @return the result of the action
    */
  final def apply(state: PipelineState) : T = {
    require(isSupported(state))
    run(state)
  }

  /**
    * Check whether the given state is supported. When an action is run on a
    * supported state, it must not fail for reasons other than IO or network errors.
    *
    * @param state the state
    * @return true iff the state is supported.
    */
  def isSupported(state: PipelineState): Boolean

  /**
    * Run the action on a supported state.
    * 
    * Running an action may have side effects but must not fail for reasons
    * other than IO or network errors. This method must not be called on an
    * unsupported state.
    *
    * @param state The state when the action is run. The state must be supported.
    * @return the result of the action
    */
  def run(state: PipelineState): T
}

/**
  * A step in the pipeline.
  * 
  * It reads the state before its execution and then returns the modified state.
  */
trait PipelineStep extends PipelineAction[PipelineState]

/**
  * The final step in a pipeline.
  * 
  * It reads the state before its execution and returns the PartitionFolders
  * it has written based on it.
  */
trait PipelineSink extends PipelineAction[Seq[PartitionFolder]]