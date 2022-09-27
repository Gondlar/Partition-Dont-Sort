package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import org.apache.spark.sql.DataFrame

trait Heuristic {

  /**
    * Choose the PathKey most suited according to this heuristic
    *
    * @param metric the calculator used to retrieve ColumnMetrics
    * @param data the data to analyze
    * @param metadata the metadata available for the current split.
    *                 A path is allowable if it does not contain any of the
    *                 known absent paths as a prefix and is not a prefix of a
    *                 known present path
    * @param thresh threshold for path presence and absence, i.e., a path is allowable
    *               if it is present in at least thresh percent of documents and absent in
    *               at least thresh percent of documents 
    * @return The best PathKey or None if no paths are allowable
    */
  def choose(
    metric: PartitionMetricCalculator,
    df: DataFrame,
    metadata: PartitionMetadata,
    thresh : Double
  ): Option[PathKey]
}

/**
  * Utility class for Heuristics which handles allowable paths and edge cases
  */
abstract class AbstractHeuristic extends Heuristic {

  /**
    * Determine the value to use to to find the best row.
    * The AbstractHeuristic will return the PathKey where this value is higest
    *
    * @param col the metic
    * @return the value
    */
  protected def heuristic(col: ColumnMetric): Int

  override def choose(
    metric: PartitionMetricCalculator,
    df: DataFrame,
    metadata: PartitionMetadata,
    thresh : Double
  ): Option[PathKey] = {
    val paths = allowablePaths(metric, df, metadata, thresh)
    if (paths.isEmpty) None else {
      Some(paths.maxBy(heuristic)._1)
    }
  }

  /**
    * Find all allowable paths for a given dataframe
    *
    * @param metric the calculator used to retrieve ColumnMetrics
    * @param data the data to analyze
    * @param metadata the metadata available for the current split.
    *                 A path is allowable if it does not contain any of the
    *                 known absent paths as a prefix and is not a prefix of a
    *                 known present path
    * @param thresh threshold for path presence and absence, i.e., a path is allowable
    *               if it is present in at least thresh percent of documents and absent in
    *               at least thresh percent of documents 
    * @return A list of allowable paths and their heuristics
    */
  protected def allowablePaths(
    metric: PartitionMetricCalculator,
    df: DataFrame,
    metadata: PartitionMetadata,
    thresh : Double
  ): Iterator[ColumnMetric] = {
    val (size, presentCount, switchCount) = metric.calc(df)
    val min = (thresh*size).ceil.toInt
    val cutoff = (size/2) - min

    metric.paths(df)
          .zip(presentCount.values.iterator)
          .zip(switchCount.values.iterator)
          .filter({ case ((path,present),_) =>
            present < cutoff && !metadata.isKnown(path)
          })
          .map({ case ((path, present), switch) => (path, present, switch)})
  }
}

/**
  * Find the best path to split using the even heuristic
  *
  */
object EvenHeuristic extends AbstractHeuristic {
  override protected def heuristic(col: ColumnMetric): Int = -col._2
}

/**
  * Find the best path to split using the switch heuristic
  *
  */
object SwitchHeuristic extends AbstractHeuristic {
  override protected def heuristic(col: ColumnMetric): Int = col._3
}
