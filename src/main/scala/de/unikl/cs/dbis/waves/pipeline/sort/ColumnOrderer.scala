package de.unikl.cs.dbis.waves.pipeline.sort

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

import de.unikl.cs.dbis.waves.pipeline.PipelineState

trait ColumnOrderer {
  def isSupported(state: PipelineState): Boolean
  def sort(state: PipelineState, df: DataFrame): Seq[Column]
}
