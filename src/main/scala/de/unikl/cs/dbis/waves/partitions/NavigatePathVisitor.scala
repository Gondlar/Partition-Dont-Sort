package de.unikl.cs.dbis.waves.partitions

case class InvalidPathException(
  val path : Seq[PartitionTreePath],
  val step: PartitionTreePath,
  private val message : String = "Wrong kind of step for node",
  private val cause : Throwable = null
) extends Exception(s"Could not take $step in $path: $message", cause)

/**
  * Navigate along a path
  *
  * @param path the path
  */
abstract class NavigatePathVisitor[Payload](
    path : Iterable[PartitionTreePath]
) extends PartitionTreeVisitor[Payload] {
    private val iterator = path.iterator

    /**
      * This method is called when the path does not fit the tree we are
      * visiting. The default implementation throws an exception. Note that
      * chaning the default behaviour makes the Visitor call navigateUp after
      * an invalid path.
      *
      * @param node the node where we could not take the step
      */
    def invalidStep(node: TreeNode[Payload], step: PartitionTreePath): Unit = node match {
      case Bucket(data) => throw InvalidPathException(path.toSeq, step, "Reached leaf before end of path")
      case _ => throw InvalidPathException(path.toSeq, step)
    }

    /**
      * This method is called when we have reached the end of the path.
      * Its default implementation does nothing
      *
      * @param node the node the path refers to
      */
    protected def endOfPath(node: TreeNode[Payload]): Unit = {}

    /**
      * This method is called when traversing the path forwards
      * Its default implementation does nothing
      *
      * @param from the node we are at
      * @param to the node we are going to
      * @param step the step we are taking
      */
    protected def navigateDown(from: TreeNode[Payload], to: TreeNode[Payload])(step: from.PathType) = {}

    /**
      * This method is called when traversing the path backwards
      * Its default implementation does nothing
      *
      * @param from the node we are at
      * @param to the node we are returning to
      * @param step the step we took
      */
    protected def navigateUp(from: TreeNode[Payload], to: TreeNode[Payload])(step: to.PathType) = {}

    override def visit(bucket: Bucket[Payload]): Unit
        = if (iterator.hasNext) invalidStep(bucket, iterator.next())
          else endOfPath(bucket)

    override def visit(node: SplitByPresence[Payload]): Unit
      = navigate(node)

    override def visit(root: Spill[Payload]): Unit
      = navigate(root)

    private def navigate(from: TreeNode[Payload]): Unit = {
      if (!iterator.hasNext) {
        endOfPath(from)
        return
      }

      val next = iterator.next
      val to = from.navigate.applyOrElse(next, {s: PartitionTreePath => 
        invalidStep(from, s)
        return // this returns from navigate, not the lambda
      })
      
      val step = next.asInstanceOf[from.PathType]
      navigateDown(from, to)(step)
      to.accept(this)
      navigateUp(to, from)(step)
    }
}
