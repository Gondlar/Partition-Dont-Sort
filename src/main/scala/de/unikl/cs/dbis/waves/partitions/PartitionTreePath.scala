package de.unikl.cs.dbis.waves.partitions

/**
  * Navigation elements for paths in a PartitionTree
  */
sealed trait PartitionTreePath {
  override def toString(): String = this.getClass().getSimpleName().init
}

/**
  * Subtype for all steps that can be taken from a [[Bucket]]
  * 
  * Surprise: it has no concrete instances because a bucket never has children
  */
sealed trait BucketPath extends PartitionTreePath

/**
  * Subtype for all steps that can be taken from a [[SplitByPresence]]
  */
sealed trait SplitByPresencePath extends PartitionTreePath

/**
  * Subtype for all steps that can be taken from a [[SplitByValue]]
  */
sealed trait ValuePath extends PartitionTreePath

/**
  * Subtype for all steps that can be taken from a [[Spill]]
  */
sealed trait SpillPath extends PartitionTreePath

/**
  * Subtype for all steps that can be taken from a [[Spill]]
  */
sealed case class NWayPath(position: Int) extends PartitionTreePath {
  require(position >= 0)
}

/**
  * Navigate to the "present" side of a SplitByPresence node
  */
object Present extends SplitByPresencePath

/**
  * Navigate to the "absent" side of a SplitByPresence
  */
object Absent extends SplitByPresencePath

/**
  * Navigate to the "less" side of a SplitByValue
  */
object Less extends ValuePath

/**
  * Navigate to the "more" side of a SplitByValue
  */
object MoreOrNull extends ValuePath

/**
  * Navigate to the "partitioned" side of a Spill
  */
object Partitioned extends SpillPath

/**
  * Navigate to the "rest" side of a Spill
  */
object Rest extends SpillPath