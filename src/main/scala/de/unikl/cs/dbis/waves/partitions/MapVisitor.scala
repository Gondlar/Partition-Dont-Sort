package de.unikl.cs.dbis.waves.partitions

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
extends PartitionTreeVisitor[From] {
    private var visitedBuckets = 0
    private var result : AnyNode[To] = null

    override def visit(bucket: Bucket[From]) : Unit = {
      result = Bucket(func(bucket.data, visitedBuckets))
      visitedBuckets += 1
    }

    override def visit(node: SplitByPresence[From]) : Unit = {
      node.absentKey.accept(this)
      val absent = result
      node.presentKey.accept(this)
      result = SplitByPresence(node.key, result, absent)  
    }

    override def visit(spill: Spill[From]) : Unit = {
      spill.rest.accept(this)
      val rest = result.asInstanceOf[Bucket[To]]
      spill.partitioned.accept(this)
      result = Spill(result, rest)
    }

    def getResult = {
        assert(visitedBuckets != 0) // there are no trees without buckets, so 0 visited buckets means visitor didn't run
        result
    }

    /**
      * @return the number of buckets visited
      */
    def getBucketCount = visitedBuckets
}