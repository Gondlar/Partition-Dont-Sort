package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline.PipelineState

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

final case class DummyColumnOrderer(
  supported: Boolean,
  order: Seq[Column] = Seq.empty
) extends ColumnOrderer {

  override def supports(state: PipelineState): Boolean = supported

  override def sort(state: PipelineState, df: DataFrame): Seq[Column]
    = order

}
