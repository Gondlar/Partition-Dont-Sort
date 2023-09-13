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
      case f: SplitByValue[Payload] => step match {
        case Less => metadata.addFiltered(f.key, Less)
        case MoreOrNull => metadata.addStep(MoreOrNull)
      }
      case _ => metadata.addStep(step)
    }
  }
}

trait MetadataForPathOperations {
  implicit class MetadataForPathNode[Payload](node: AnyNode[Payload]) {
    /**
      * Get all paths known to be absent or present in the subtree rooted in
      * the node referenced by the given path.
      *
      * @param path the path
      * @return a tuple with lists of the absent and present paths
      */
    def metadataFor(path : Iterable[PartitionTreePath])
      = node(new MetadataForPathVisitor[Payload](path))
  }
  implicit class MetadataForPathTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Get all paths known to be absent or present in the subtree rooted in
      * the node referenced by the given path.
      *
      * @param path the path
      * @return a tuple with lists of the absent and present paths
      */
    def metadataFor(path : Iterable[PartitionTreePath])
      = tree.root(new MetadataForPathVisitor[Payload](path))
  }
}
