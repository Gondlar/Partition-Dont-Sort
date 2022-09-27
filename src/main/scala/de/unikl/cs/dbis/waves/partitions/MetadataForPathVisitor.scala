package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.util.PathKey

import TreeNode.AnyNode

/**
  * Find the node referenced by a path
  *
  * @param path the path
  */
final class MetadataForPathVisitor[Payload](
    path : Iterable[PartitionTreePath]
) extends NavigatePathVisitor[Payload](path) {
  private val metadata = PartitionMetadata()

  def getMetadata = metadata

  override protected def navigateDown[Step <: PartitionTreePath, From <: TreeNode[Payload,Step]](
    from: From, to: AnyNode[Payload], step: Step
  ) = {
    from match {
      case f: SplitByPresence[Payload] => step match {
        case Absent => metadata.addAbsent(f.key)
        case Present => metadata.addPresent(f.key)
      }
      case _ => {}
    }
  }
}
