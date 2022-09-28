package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

final class FindBucketVisitor[Payload](
    row : InternalRow,
    schema : StructType
) extends PartitionTreeVisitor[Payload] {
    
    private var res : Bucket[Payload] = null
    def result = res

    override def visit(bucket: Bucket[Payload]): Unit
        = res = bucket

    override def visit(node: SplitByPresence[Payload]): Unit
        = (if (node.key.present(row, schema)) node.presentKey else node.absentKey).accept(this)

    override def visit(root: Spill[Payload]): Unit = root.partitioned.accept(this)
}
