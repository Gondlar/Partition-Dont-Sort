package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import scala.collection.mutable.ArrayBuffer

import TreeNode.AnyNode

/**
  * Visitor to find Metadata for all Buckets in a PartitionTree
  * @param initialMetadata the Metadata if the root. This is useful, e.g., when
  *                        visiting only a subtree.
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

  override def visit(node: SplitByValue[Payload]) : Unit = {
    val secondMetadata = metadata.clone
    metadata.addFiltered(node.key, Less)
    node.less.accept(this)

    metadata = secondMetadata
    metadata.addStep(MoreOrNull)
    node.more.accept(this)
  }

  override def visit(spill: Spill[Payload]) : Unit = {
    val rest = metadata.clone
    rest.addStep(Rest)
    builder += rest
    metadata.addStep(Partitioned)
    spill.partitioned.accept(this)
  }

  override def visit(nway: EvenNWay[Payload]): Unit = {
    val currentMetadata = metadata
    for ((child, index) <- nway.children.zipWithIndex) {
      metadata = currentMetadata.clone()
      metadata.addStep(NWayPath(index))
      child.accept(this)
    }
  }

  override def result = builder.result
}

trait CollectBucketMetadataOperations {

  implicit class CollectBucketMetadataNode[Payload](node: AnyNode[Payload]) {
    /**
      * Find the metadata for all Buckets in the tree
      *
      * @return the metadata as a sequence in the same order as getBuckets
      */
    def metadata(initialMetadata: PartitionMetadata = PartitionMetadata())
      = node(new CollectBucketMetadataVisitor[Payload](initialMetadata))
  }
  implicit class CollectBucketMetadataTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Find the metadata for all Buckets in the tree
      *
      * @return the metadata as a sequence in the same order as getBuckets
      */
    def metadata(initialMetadata: PartitionMetadata = PartitionMetadata())
      = tree.root(new CollectBucketMetadataVisitor[Payload](initialMetadata))
  }
}
