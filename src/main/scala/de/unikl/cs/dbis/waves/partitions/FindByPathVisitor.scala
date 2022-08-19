package de.unikl.cs.dbis.waves.partitions

/**
  * Find the node referenced by a path
  *
  * @param path the path
  */
final class FindByPathVisitor[Payload](
    path : Iterable[PartitionTreePath]
) extends PartitionTreeVisitor[Payload] {
    private var iterator = path.iterator
    
    private var res : Option[TreeNode[Payload]] = None
    def result = res

    override def visit(bucket: Bucket[Payload]): Unit
        = if (iterator.hasNext) res = None
          else res = Some(bucket)

    override def visit(node: SplitByPresence[Payload]): Unit
        = if (iterator.hasNext) {
            iterator.next match {
                case Present => node.presentKey.accept(this)
                case Absent => node.absentKey.accept(this)
                case _ => res = None
            }
        } else res = Some(node)

    override def visit(root: Spill[Payload]): Unit
        = if (iterator.hasNext) {
            iterator.next match {
                case Partitioned => root.partitioned.accept(this)
                case Rest => root.rest.accept(this)
                case _ => res = None
            }
        } else res = Some(root)
}
