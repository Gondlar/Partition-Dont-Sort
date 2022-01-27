package de.unikl.cs.dbis.waves.partitions

import scala.collection.mutable.ArrayBuffer

final class CollectBucketsVisitor() extends PartitionTreeVisitor {
    private val buckets = ArrayBuffer.empty[Bucket]

    override def visit(bucket: Bucket) : Unit = buckets.addOne(bucket)

    override def visit(node: PartitionByInnerNode) : Unit = {
        node.absentKey.accept(this)
        node.presentKey.accept(this)
    }

    override def visit(spill: Spill) : Unit = {
        buckets.addOne(spill.rest)
        spill.partitioned.accept(this)
    }

    def iter = buckets.iterator
}