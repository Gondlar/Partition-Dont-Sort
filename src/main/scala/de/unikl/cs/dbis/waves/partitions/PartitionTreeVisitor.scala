package de.unikl.cs.dbis.waves.partitions

trait PartitionTreeVisitor {
  def visit(bucket: Bucket) : Unit
  def visit(node: PartitionByInnerNode) : Unit
  def visit(root: Spill) : Unit
}
