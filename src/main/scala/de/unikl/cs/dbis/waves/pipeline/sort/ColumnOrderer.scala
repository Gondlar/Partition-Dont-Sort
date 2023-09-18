package de.unikl.cs.dbis.waves.pipeline.sort

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

import de.unikl.cs.dbis.waves.pipeline.PipelineState

/**
  * Trait for all Algorithms which determine a (local or global) column order
  * for recursive sorting.
  */
trait ColumnOrderer {

  /**
    * Check whether this Orderer is supported in the current state
    *
    * @param state the state
    * @return true iff it is supported
    */
  def supports(state: PipelineState): Boolean

  /**
    * Return a column order for the given DataFrame and state
    *
    * @param state the state
    * @param df the DataFrame to find the order for
    * @return a List of Spark Columns that are to be used for sorting.
    */
  def sort(state: PipelineState, df: DataFrame): Seq[Column]
  //TODO: Is df enough to distinguish between Buckets for all ColumnOrderers we consider?
}

/**
  * A mixin to mark orderers which are always supported
  */
trait NoPrerequisites extends ColumnOrderer {
  final override def supports(state: PipelineState) = true
}
