package de.unikl.cs.dbis.waves.partitions

/**
  * Find the node referenced by a path
  *
  * @param path the path
  */
final class FindByPathVisitor[Payload](
    path : Iterable[PartitionTreePath]
) extends NavigatePathVisitor[Payload](path) {
    private var res : Option[TreeNode[Payload]] = None

    def result = res

    override protected def endOfPath(node: TreeNode[Payload]): Unit
      = res = Some(node)

    override def invalidStep(node: TreeNode[Payload], step: PartitionTreePath): Unit
      = res = None
}
