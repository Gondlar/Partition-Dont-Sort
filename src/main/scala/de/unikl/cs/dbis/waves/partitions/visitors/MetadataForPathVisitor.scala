package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.util.PathKey

import TreeNode.AnyNode

/**
  * Find the node referenced by a path
  *
  * @param path the path
  */
final class MetadataForPathVisitor[Payload](
    path : Iterable[PartitionTreePath]
) extends NavigatePathVisitor[Payload](path) with SingleResultVisitor[Payload,PartitionMetadata] {
  private val metadata = PartitionMetadata()

  override def result = metadata

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
