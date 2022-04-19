package de.unikl.cs.dbis.waves.autosplit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.PathKey

object AutosplitCalculator {
    type PartitionMetric = (Int, ObjectCounter, ObjectCounter)
  
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
  
    private def reduce(lhs : PartitionMetric, rhs : PartitionMetric) : PartitionMetric = {
        var (rowCount, presentCount, switchCount) = lhs
        rowCount += rhs._1
        presentCount += rhs._2
        switchCount += rhs._3
        (rowCount, presentCount, switchCount)
    }
  
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

    def switchHeuristic(data : DataFrame, knownAbsent: Seq[PathKey], knownPresent: Seq[PathKey], min : Int) = {
        val paths = calculate(data, knownAbsent, knownPresent, min)
        if (paths.isEmpty) None else {
            Some(paths.maxBy({case (_, _, switch) => switch})._1)
        }
    }

    def evenHeuristic(data : DataFrame, knownAbsent: Seq[PathKey], knownPresent: Seq[PathKey], min : Int) = {
        val paths = calculate(data, knownAbsent, knownPresent, min)
        if (paths.isEmpty) None else {
            Some(paths.minBy({case (_, even, _) => even})._1)
        }
    }
}
