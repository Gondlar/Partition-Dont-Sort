package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import TreeNode.AnyNode

final class FindBucketVisitor[Payload](
    row : InternalRow,
    schema : StructType
) extends SingleResultVisitor[Payload,Bucket[Payload]] {
    
    private var res : Bucket[Payload] = null
    override def result = res

    override def visit(bucket: Bucket[Payload]): Unit
        = res = bucket

    override def visit(node: SplitByPresence[Payload]): Unit
        = (if (node.key.present(row, schema)) node.presentKey else node.absentKey).accept(this)

    override def visit(node: SplitByValue[Payload]): Unit
        = ??? //TODO

    override def visit(root: Spill[Payload]): Unit = root.partitioned.accept(this)
}

trait FindBucketOperations {
  implicit class FindBucketsNode[Payload](node: AnyNode[Payload]) {
    /**
      * Find the Bucket a Row belongs to
      *
      * @param row the row
      * @return the bucket
      */
    def bucketFor(row : InternalRow, schema: StructType)
      = node(new FindBucketVisitor[Payload](row, schema))
  }
  implicit class FindBucketsTree[Payload](tree: PartitionTree[Payload]) {
    /**
      * Find the Bucket a Row belongs to
      *
      * @param row the row
      * @return the bucket
      */
    def bucketFor(row : InternalRow)
      = tree.root(new FindBucketVisitor[Payload](row, tree.globalSchema))
  }
}
