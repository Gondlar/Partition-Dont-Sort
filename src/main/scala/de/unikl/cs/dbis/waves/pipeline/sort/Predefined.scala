package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline.PipelineState

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

final case class Predefined(
  order: Seq[Column]
) extends ColumnOrderer {

  override def supports(state: PipelineState): Boolean = true

  override def sort(state: PipelineState, df: DataFrame): Seq[Column] = order

}
