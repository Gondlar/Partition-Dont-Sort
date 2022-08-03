package de.unikl.cs.dbis.waves.partitions

/**
  * Navigation elements for paths in a PartitionTree
  */
sealed trait PartitionTreePath

/**
  * Navigate to the "present" side of a SplitByPresence node
  */
object Present extends PartitionTreePath

/**
  * Navigate to the "absent" side of a SplitByPresence
  */
object Absent extends PartitionTreePath

/**
  * Navigate to the "partitioned" side of a Spill
  */
object Partitioned extends PartitionTreePath

/**
  * Navigate to the "rest" side of a Spill
  */
object Rest extends PartitionTreePath