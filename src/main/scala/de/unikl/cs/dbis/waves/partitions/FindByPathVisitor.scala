package de.unikl.cs.dbis.waves.partitions

import TreeNode.AnyNode

/**
  * Find the node referenced by a path
  *
  * @param path the path
  */
final class FindByPathVisitor[Payload](
    path : Iterable[PartitionTreePath]
) extends NavigatePathVisitor[Payload](path) {
    private var res : Option[AnyNode[Payload]] = None

    def result = res

    override protected def endOfPath(node: AnyNode[Payload]): Unit
      = res = Some(node)

    override def invalidStep(node: AnyNode[Payload], step: PartitionTreePath): Unit
      = res = None
}
