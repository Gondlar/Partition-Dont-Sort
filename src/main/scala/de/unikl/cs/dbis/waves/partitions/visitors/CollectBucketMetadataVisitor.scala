package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import scala.collection.mutable.ArrayBuffer

/**
  * Visitor to find Metadata for all Buckets in a PartitionTree
  */
final class CollectBucketMetadataVisitor[Payload](
  initialMetadata: PartitionMetadata = PartitionMetadata(),
) extends SingleResultVisitor[Payload,Seq[PartitionMetadata]] {
  private val builder = Seq.newBuilder[PartitionMetadata]
  private var metadata = initialMetadata.clone()

  override def visit(bucket: Bucket[Payload]) : Unit = builder += metadata.clone

  override def visit(node: SplitByPresence[Payload]) : Unit = {
    val secondMetadata = metadata.clone
    metadata.addAbsent(node.key)
    node.absentKey.accept(this)

    metadata = secondMetadata
    metadata.addPresent(node.key)
    node.presentKey.accept(this)
  }

  override def visit(spill: Spill[Payload]) : Unit = {
    builder += metadata.clone
    spill.partitioned.accept(this)
  }

  override def result = builder.result
}