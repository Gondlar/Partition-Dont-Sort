package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata

/**
  * This step prepends Metadata to the one stored in the state
  *
  * @param metadata
  */
final case class PrependMetadata(
  metadata: PartitionMetadata
) extends PipelineStep with NoPrerequisites {

  override def run(state: PipelineState): PipelineState
    = KnownMetadata(state) = metadata ++ KnownMetadata(state)

}
