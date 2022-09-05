package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.util.PathKey

/**
  * Methods which calculate Heuristics
  */
package object recursive {
    /**
      * Type alias for summary statistics within a spark partition
      */
    type PartitionMetric = (Int, ObjectCounter, ObjectCounter)

    /**
      * Type alias for summary statistics of a DataFrame's column
      */
    type ColumnMetric = (PathKey, Int, Int)
}
