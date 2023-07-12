package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._

/**
  * This pipeline step ensures that there is just one spark partition per bucket.
  * As such, Buckets must be defined in the state.
  * 
  * This modification is sometimes necessary to ensure that there is just one
  * Parquet file per Bucket but can be detrimental to performance if this is not
  * required due to loss of parallelism.
  * 
  * This step is automatically run by the Pipeline if finalization is requested,
  * but can also be manually inserted into more complex pipelines where multiple
  * finalization steps are required or the finalization is not the last step in
  * the pipeline.
  */
object Finalizer extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = Buckets isDefinedIn state

  override def run(state: PipelineState): PipelineState
    = Buckets(state) = Buckets(state).map(_.repartition(1))
}
