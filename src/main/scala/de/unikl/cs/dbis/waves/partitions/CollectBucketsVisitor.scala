package de.unikl.cs.dbis.waves.partitions

import scala.collection.mutable.ArrayBuffer

/**
  * Visitor to find all Buckets in a PartitionTree
  *
  */
final class CollectBucketsVisitor[Payload]() extends PartitionTreeVisitor[Payload] {
    private val buckets = ArrayBuffer.empty[Bucket[Payload]]

    override def visit(bucket: Bucket[Payload]) : Unit = buckets += bucket

    override def visit(node: SplitByPresence[Payload]) : Unit = {
        node.absentKey.accept(this)
        node.presentKey.accept(this)
    }

    override def visit(spill: Spill[Payload]) : Unit = {
        buckets += spill.rest
        spill.partitioned.accept(this)
    }

    def iter = buckets.iterator
}