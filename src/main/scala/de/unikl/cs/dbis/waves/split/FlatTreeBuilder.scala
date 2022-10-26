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
  override protected def buildTree(folders: Seq[PartitionFolder]): PartitionTree[String] = {
    val buckets = folders.map(f => Bucket(f.name))
    val tree = buckets.tail.foldLeft(buckets.head: AnyNode[String])((partitioned,bucket) => Spill(partitioned,bucket))
    new PartitionTree(data.schema, sorter, tree)
  }
}
