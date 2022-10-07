package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import TreeNode.AnyNode

/**
  * Find the node referenced by a path
  *
  * @param path the path
  */
final class FindByPathVisitor[Payload](
    path : Seq[PartitionTreePath]
) extends NavigatePathVisitor[Payload](path) with SingleResultVisitor[Payload,Option[AnyNode[Payload]]] {
    private var res : Option[AnyNode[Payload]] = None

    override def result = res

    override protected def endOfPath(node: AnyNode[Payload]): Unit
      = res = Some(node)

    override def invalidStep(node: AnyNode[Payload], step: PartitionTreePath): Unit
      = res = None
}

trait FindByPathOperations {
  implicit class FindByPathNode[Payload](node: AnyNode[Payload]) {
    /**
      * Get a node represented by navigating along a path.
      * The path consists of String representing the navigational choices
      *
      * @param path the path
      * @return the node at the end of the path or None of no such node exists
      */
    def find(path: Seq[PartitionTreePath]) = node(new FindByPathVisitor[Payload](path))
  }
  implicit class FindByPathTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Get a node represented by navigating along a path.
      * The path consists of String representing the navigational choices
      *
      * @param path the path
      * @return the node at the end of the path or None of no such node exists
      */
    def find(path: Seq[PartitionTreePath]) = tree.root(new FindByPathVisitor[Payload](path))
  }
}
