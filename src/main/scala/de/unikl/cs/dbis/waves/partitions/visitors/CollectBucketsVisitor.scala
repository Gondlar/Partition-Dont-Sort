package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import scala.collection.mutable.ArrayBuffer

import TreeNode.AnyNode

/**
  * Visitor to find all Buckets in a PartitionTree
  */
final class CollectBucketsVisitor[Payload]() extends SingleResultVisitor[Payload,Seq[Bucket[Payload]]] {
    private val buckets = Seq.newBuilder[Bucket[Payload]]

    override def visit(bucket: Bucket[Payload]) : Unit = buckets += bucket

    override def visit(node: SplitByPresence[Payload]) : Unit = {
        node.absentKey.accept(this)
        node.presentKey.accept(this)
    }

    override def visit(node: SplitByValue[Payload]) : Unit = {
        node.less.accept(this)
        node.more.accept(this)
    }

    override def visit(spill: Spill[Payload]) : Unit = {
        buckets += spill.rest
        spill.partitioned.accept(this)
    }

    override def visit(nway: EvenNWay[Payload]): Unit = {
      for (child <- nway.children)
        child.accept(this)
    }

    override def result: Seq[Bucket[Payload]] = buckets.result
}

trait CollectBucketOperations {
  implicit class CollectBucketsNode[Payload](node: AnyNode[Payload]) {
    /**
      * Find all Buckets in the PartitionTree
      *
      * @return an iterator of Buckets
      */
    def buckets = node(new CollectBucketsVisitor[Payload])
  }
  implicit class CollectBucketsTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Find all Buckets in the PartitionTree
      *
      * @return an iterator of Buckets
      */
    def buckets = tree.root(new CollectBucketsVisitor[Payload])
  }

  implicit class CollectFoldersNode(node: AnyNode[String]) {
    /**
      * Find all Folders in the PartitionTree
      *
      * @param basePath the basePath of the folders
      * @return a sequence of folders in the tree
      */
    def folders(basePath: String) = node.buckets.map(_.folder(basePath))
  }

  implicit class CollectFoldersTree(tree: PartitionTree[String]) {
    /**
      * Find all Folders in the PartitionTree
      *
      * @param basePath the basePath of the folders
      * @return a sequence of folders in the tree
      */
    def folders(basePath: String) = tree.root.folders(basePath)
  }
}
