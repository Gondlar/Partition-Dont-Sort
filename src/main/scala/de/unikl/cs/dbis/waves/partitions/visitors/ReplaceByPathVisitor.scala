package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import TreeNode.AnyNode

/**
  * Replace the node referenced by a path
  *
  * @param path the path
  * @param replace the subtree to insert
  */
final class ReplaceByPathVisitor[Payload](
    path : Seq[PartitionTreePath],
    replace: AnyNode[Payload]
) extends NavigatePathVisitor[Payload](path) with SingleResultVisitor[Payload,AnyNode[Payload]] {
    private var res : AnyNode[Payload] = replace

    override def result = res

    override protected def navigateUp[Step <: PartitionTreePath, To <: TreeNode[Payload,Step]](
      from: AnyNode[Payload], to: To, step: Step
    ): Unit = {
      res = to match {
        case Spill(partitioned, rest) => step match {
          case Partitioned => Spill(res, rest)
          case Rest => res match {
            case bucket : Bucket[Payload] => Spill(partitioned, bucket)
            case _ => throw new ImpossibleReplacementException("cannot replace spill partition with a non-bucket")
          }
        }
        case SplitByPresence(key, presentKey, absentKey) => step match {
          case Absent => SplitByPresence(key, presentKey, res)
          case Present => SplitByPresence(key, res, absentKey)
        }
        // to can never be a bucket because it has no children
      }
    }
}
