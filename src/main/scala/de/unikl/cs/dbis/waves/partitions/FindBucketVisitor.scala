package de.unikl.cs.dbis.waves.partitions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

final class FindBucketVisitor(
    row : InternalRow,
    schema : StructType
) extends PartitionTreeVisitor {
    
    private var res : Bucket = null
    def result = res

    override def visit(bucket: Bucket): Unit
        = res = bucket

    override def visit(node: SplitByPresence): Unit
        = (if (node.key.present(row, schema)) node.presentKey else node.absentKey).accept(this)

    override def visit(root: Spill): Unit = root.partitioned.accept(this)
}
