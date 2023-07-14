package de.unikl.cs.dbis.waves.pipeline.sink

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

/**
  * Write a part of a partition tree to disk. This is useful, e.g., to
  * repartition an existing table.
  * 
  * Given the full preexisting tree and the path to its node which we want to
  * replace, we keep all Folders outside that subtree. The actual new data is
  * written using the delegate writer.
  *
  * @param delegate
  * @param oldFullTree
  * @param path
  */
final case class SubtreeSink(
  delegate: PipelineSink,
  oldFullTree: AnyNode[String],
  path: Seq[PartitionTreePath]
) extends PipelineSink {

  require(oldFullTree.find(path).isDefined)

  override def supports(state: PipelineState): Boolean = {
    // if ((KnownMetadata isDefinedIn state) && KnownMetadata(state).getPath != path)
    //   System.err.println(s"WARN: Metadata path ${KnownMetadata(state).getPath} does not match subtree path $path")
    (Shape isDefinedIn state) && (delegate supports state)
  }

  override def run(state: PipelineState): (PipelineState, Seq[PartitionFolder]) = {
    val (before, after) = pathHalfs(state.path)
    val (delegateResultState, newFolders) = delegate.run(state)
    val resultState = Shape(delegateResultState) = oldFullTree.shape.replace(path, Shape(delegateResultState))
    (resultState, before ++ newFolders ++ after)
  }

  private def pathHalfs(basePath: String) = {
    var before = Seq.empty[PartitionFolder]
    var after = Seq.empty[PartitionFolder]

    var remainingPath = path
    var remainingTree = oldFullTree
    while (!remainingPath.isEmpty) {
      (remainingTree, remainingPath.head) match {
        case (Spill(partitioned, bucket), Partitioned) => {
          before = before :+ bucket.folder(basePath)
          remainingTree = partitioned
        }
        case (Spill(partitioned, bucket), Rest) => {
          after = partitioned.folders(basePath) ++ after
          remainingTree = bucket
        }
        case (SplitByPresence(_, present, absent), Present) => {
          before = before ++ absent.folders(basePath)
          remainingTree = present
        }
        case (SplitByPresence(_, present, absent), Absent) => {
          after = present.folders(basePath) ++ after
          remainingTree = absent
        }
        case _ => assert(false, "should not be supported")
      }
      remainingPath = remainingPath.tail
    }
    (before, after)
  }

}
