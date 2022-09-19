package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.SparkSession
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.{
  Bucket, Spill, TreeNode, PartitionTree, PartitionTreeHDFSInterface
}

import TreeNode.AnyNode
import scala.annotation.tailrec

/**
  * The FlaTreeBuilder Mixin is meant for Splitters where the splits do not
  * carry information for data skipping. Instead, it degenerates the tree to a
  * list by using Spill nodes.
  */
trait FlatTreeBuilder extends GroupedSplitter {
  override protected def buildTree(folders: Seq[PartitionFolder]): PartitionTree[String]
    = new PartitionTree(data.schema, toTree(folders.map(f => Bucket(f.name))))

  // we cannot use fold because AnyNode instances may not have a common ancestor
  private def toTree(folders: Seq[Bucket[String]]): AnyNode[String] = folders match {
    case head :: Nil => head
    case head :: tail => Spill(toTree(tail), head)
  }
}
