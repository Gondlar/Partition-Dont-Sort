package de.unikl.cs.dbis.waves.partitions

final class FindByPathVisitor(
    path : Iterable[String]
) extends PartitionTreeVisitor {
    private var iterator = path.iterator
    
    private var res : Option[TreeNode] = None
    def result = res

    override def visit(bucket: Bucket): Unit
        = if (iterator.hasNext) res = None
          else res = Some(bucket)

    override def visit(node: SplitByPresence): Unit
        = if (iterator.hasNext) {
            iterator.next match {
                case SplitByPresence.PRESENT_KEY
                    => node.presentKey.accept(this)
                case SplitByPresence.ABSENT_KEY
                    => node.absentKey.accept(this)
                case _ => res = None
            }
        } else res = Some(node)

    override def visit(root: Spill): Unit
        = if (iterator.hasNext) {
            iterator.next match {
                case Spill.PARTIOTIONED_KEY
                    => root.partitioned.accept(this)
                case Spill.REST_KEY
                    => root.rest.accept(this)
                case _ => res = None
            }
        } else res = Some(root)
}
