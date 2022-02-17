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

    override def visit(node: PartitionByInnerNode): Unit
        = iterator.nextOption() match {
              case Some(PartitionByInnerNode.PRESENT_KEY)
                  => node.presentKey.accept(this)
              case Some(PartitionByInnerNode.ABSENT_KEY)
                  => node.absentKey.accept(this)
              case None => res = Some(node)
              case _ => res = None
        }

    override def visit(root: Spill): Unit
        = iterator.nextOption() match {
            case Some(Spill.PARTIOTIONED_KEY)
                => root.partitioned.accept(this)
            case Some(Spill.REST_KEY)
                => root.rest.accept(this)
            case None => res = Some(root)
            case _ => res = None
        }
}
