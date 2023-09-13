package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import TreeNode.AnyNode

/**
  * Applies a function to all payloads of buckets and returns the tree which
  * stores all results as payload. The buckets are visited in the same order as
  * with [[CollectBucketsVisitor]].
  *
  * @param func a function to be applied to all buckets
  *             its first argument is the payload, the second is the index of the
  *             respective bucket in tree order
  */
final class MapVisitor[From, To](func: (From, Int) => To)
extends SingleResultVisitor[From,AnyNode[To]] {
    private var visitedBuckets = 0
    private var theResult : AnyNode[To] = null

    override def visit(bucket: Bucket[From]) : Unit = {
      theResult = Bucket(func(bucket.data, visitedBuckets))
      visitedBuckets += 1
    }

    override def visit(node: SplitByPresence[From]) : Unit = {
      node.absentKey.accept(this)
      val absent = result
      node.presentKey.accept(this)
      theResult = SplitByPresence(node.key, result, absent)  
    }

    override def visit(node: SplitByValue[From]) : Unit = {
      node.less.accept(this)
      val less = result
      node.more.accept(this)
      theResult = node.copy(less = less, more = result)
    }

    override def visit(spill: Spill[From]) : Unit = {
      spill.rest.accept(this)
      val rest = result.asInstanceOf[Bucket[To]]
      spill.partitioned.accept(this)
      theResult = Spill(result, rest)
    }

    override def result = {
        assert(visitedBuckets != 0) // there are no trees without buckets, so 0 visited buckets means visitor didn't run
        theResult
    }

    /**
      * @return the number of buckets visited
      */
    def getBucketCount = visitedBuckets
}

trait MapOperations {
  implicit class MapNode[Payload](node: AnyNode[Payload]) {
    /**
      * Apply func to all buckets and return the resulting PartitionTree
      *
      * @param func the function
      * @return the new partition tree
      */
    def map[To](func: (Payload, Int) => To) : AnyNode[To]
      = node(new MapVisitor(func))

    /**
      * Return the shape of this tree, i.e., an equivalent tree where all Bucket
      * payloads have been replaced with the Unit type.
      *
      * @return the tree's shape
      */
    def shape = map({case _ => ()})
  }
  implicit class MapTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Apply func to all buckets and return the resulting PartitionTree
      *
      * @param func the function
      * @return the new partition tree
      */
    def map[To](func: (Payload, Int) => To) : PartitionTree[To]
      = new PartitionTree(tree.globalSchema, tree.sorter, tree.root(new MapVisitor(func)))

    /**
      * Return the shape of this tree, i.e., an equivalent tree where all Bucket
      * payloads have been replaced with the Unit type.
      *
      * @return the tree's shape
      */
    def shape = tree.root.shape

    /**
      * Apply func to all buckets. As opposed to [[map]], this function modifies
      * this tree rather than creating a new one. As a result, func is limited to
      * functions which return this trees payload type.
      *
      * @param func the function
      */
    def modify(func: (Payload, Int) => Payload) : Unit
      = tree.root = tree.root(new MapVisitor(func))
  }
}