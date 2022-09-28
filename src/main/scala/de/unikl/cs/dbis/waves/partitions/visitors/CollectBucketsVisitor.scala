package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import scala.collection.mutable.ArrayBuffer

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

    override def visit(spill: Spill[Payload]) : Unit = {
        buckets += spill.rest
        spill.partitioned.accept(this)
    }

    override def result: Seq[Bucket[Payload]] = buckets.result
}