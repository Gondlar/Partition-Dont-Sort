package de.unikl.cs.dbis.waves.autosplit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.PathKey

/**
  * Methods which calculate Heuristics
  */
object AutosplitCalculator {
    /**
      * Type alias for summary statistics within a spark partition
      */
    type PartitionMetric = (Int, ObjectCounter, ObjectCounter)
  
    /**
      * Calculate statistics for one spark partition
      *
      * @param partition iterator over the partition's entries
      * @param optionalCount The number of optional nodes in the schema
      * @return the statistics for this partition
      */
    private def combine(partition : Iterator[Row], optionalCount : Int) : Iterator[PartitionMetric] = {
        if (partition.hasNext) {
            val presentCount = ObjectCounter(optionalCount)
            val switchCount = ObjectCounter(optionalCount)
            var old = ObjectCounter(optionalCount)
            var current = ObjectCounter(optionalCount)
            var rowCount = 1
  
            old <-- PresentMetric(partition.next())
            for (row <- partition) {
                rowCount += 1
                presentCount += old
                current <-- PresentMetric(row)
                old <-- SwitchMetric(current, old)
                switchCount += old
                val tmp = old
                old = current
                current = tmp
            }
            presentCount += old
  
            Iterator((rowCount, presentCount, switchCount))
        } else {
            // partition is empty
            Iterator((0, ObjectCounter(optionalCount), ObjectCounter(optionalCount)))
        }
    }
  
    /**
      * Combine the statistics from two partitions into one
      *
      * @param lhs the first partition's stats
      * @param rhs the second partition's stats
      * @return the combined statistics
      */
    private def reduce(lhs : PartitionMetric, rhs : PartitionMetric) : PartitionMetric = {
        var (rowCount, presentCount, switchCount) = lhs
        rowCount += rhs._1
        presentCount += rhs._2
        switchCount += rhs._3
        (rowCount, presentCount, switchCount)
    }
  
    /**
      * Find all allowable paths for a given dataframe
      *
      * @param data the data to analyze
      * @param knownAbsent paths known to be absent, i.e., a path is allowable if
                           it does not contain any of these paths as a prefix
      * @param knownPresent paths known to be present, i.e., a path is allowable if
                            it is not equal to one of these paths
      * @param min threshold for path presence and absence, i.e., a path is allowable
                   if it is present in at least min documents and absent in at least
                   min documents 
      * @return A list of allowable paths and their heuristics
      */
    def calculate(data : DataFrame, knownAbsent: Seq[PathKey], knownPresent: Seq[PathKey], min : Int) = {
        val schema = data.schema
        val optionalCount = ObjectCounter.countOptional(schema)
        var (size, presentCount, switchCount) = data.rdd
            .mapPartitions(combine(_, optionalCount))
            .reduce(reduce _)
        
        val leafCount = ObjectCounter(optionalCount)
        leafCount <-- LeafMetric(schema)
        switchCount *= leafCount
        presentCount -= (size/2)

        val cutoff = (size/2) - min
        ObjectCounter.paths(schema)
                     .zip(presentCount.values)
                     .zip(switchCount.values)
                     .filter({ case ((path,present),_) =>
                         present.abs < cutoff &&
                         !knownAbsent.map(_.contains(path)).fold(false)(_||_) && !knownPresent.map(_==path).fold(false)(_||_)
                     })
                     .map({ case ((path, present), switch) => (path, present, switch)})
    }

    /**
      * Find the best path to split using the switch heuristic
      *
      * @param data the data frame
      * @param knownAbsent the known absent paths
      * @param knownPresent the known present paths
      * @param min the threshold
      * @return the best path using the switch heuristic if such a path exists, otherwise None
      * @see [[calculate]] for the parameter documentation
      */
    def switchHeuristic(data : DataFrame, knownAbsent: Seq[PathKey], knownPresent: Seq[PathKey], min : Int) = {
        val paths = calculate(data, knownAbsent, knownPresent, min)
        if (paths.isEmpty) None else {
            Some(paths.maxBy({case (_, _, switch) => switch})._1)
        }
    }

    /**
      * Find the best path to split using the even heuristic
      *
      * @param data the data frame
      * @param knownAbsent the known absent paths
      * @param knownPresent the known present paths
      * @param min the threshold
      * @return the best path using the even heuristic if such a path exists, otherwise None
      * @see [[calculate]] for the parameter documentation
      */
    def evenHeuristic(data : DataFrame, knownAbsent: Seq[PathKey], knownPresent: Seq[PathKey], min : Int) = {
        val paths = calculate(data, knownAbsent, knownPresent, min)
        if (paths.isEmpty) None else {
            Some(paths.minBy({case (_, even, _) => even})._1)
        }
    }
}
