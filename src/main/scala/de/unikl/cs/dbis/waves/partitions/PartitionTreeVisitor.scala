package de.unikl.cs.dbis.waves.partitions

trait PartitionTreeVisitor[Payload] {
  def visit(bucket: Bucket[Payload]) : Unit
  def visit(node: SplitByPresence[Payload]) : Unit
  def visit(root: Spill[Payload]) : Unit
}
