package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.util.PathKey

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

  override protected def navigateDown(from: TreeNode[Payload], to: TreeNode[Payload], step: PartitionTreePath) = {
    from match {
      case SplitByPresence(key, _, _) => (step: @unchecked) match {
        // NavigatePathVisitor already assures we only get fitting path steps
        case Absent => absentBuilder += key
        case Present => presentBuilder += key
      }
      case _ => {}
    }
  }
}
