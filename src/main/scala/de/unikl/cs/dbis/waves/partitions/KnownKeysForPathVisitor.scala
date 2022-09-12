package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.util.PathKey

import TreeNode.AnyNode

/**
  * Find the node referenced by a path
  *
  * @param path the path
  */
final class KnownKeysForPathVisitor[Payload](
    path : Iterable[PartitionTreePath]
) extends NavigatePathVisitor[Payload](path) {
  private var absentBuilder = Seq.newBuilder[PathKey]
  private var presentBuilder = Seq.newBuilder[PathKey]

  absentBuilder.sizeHint(path.size)
  presentBuilder.sizeHint(path.size)

  def absent = absentBuilder.result()
  def present = presentBuilder.result()

  override protected def navigateDown[Step <: PartitionTreePath, From <: TreeNode[Payload,Step]](
    from: From, to: AnyNode[Payload], step: Step
  ) = {
    from match {
      case f: SplitByPresence[Payload] => step match {
        case Absent => absentBuilder += f.key
        case Present => presentBuilder += f.key
      }
      case _ => {}
    }
  }
}
