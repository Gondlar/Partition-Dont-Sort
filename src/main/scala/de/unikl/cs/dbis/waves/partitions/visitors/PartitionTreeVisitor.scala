package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

trait PartitionTreeVisitor[-Payload] {
  def visit(bucket: Bucket[Payload]) : Unit
  def visit(node: SplitByPresence[Payload]) : Unit
  def visit(node: SplitByValue[Payload]): Unit
  def visit(root: Spill[Payload]) : Unit
}
