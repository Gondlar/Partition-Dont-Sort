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