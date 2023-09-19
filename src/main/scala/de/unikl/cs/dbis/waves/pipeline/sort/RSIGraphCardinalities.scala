package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

object RSIGRaphCardinalities extends ColumnOrderer {

  override def supports(state: PipelineState): Boolean
    = StructureMetadata isDefinedIn state

  override def sort(state: PipelineState, df: DataFrame): Seq[Column] = {
    val (dlColumns, columns) = colsFromRSIGraph(StructureMetadata(state))
    val spark1 = dlColumns.map{case (path, distinct) => (ExactCardinalities.definitionLevel(path.get), distinct)}
    val spark2 = columns.map{case (path, distinct) => (path.get.toCol, distinct)}
    (spark1 ++ spark2).filter(_._2 > 1).sortBy(_._2).map(_._1)
  }

  def colsFromRSIGraph(graph: RSIGraph): (Seq[(Option[PathKey], Long)], Seq[(Option[PathKey], Long)]) = {
    if (graph.children.isEmpty) {
      val leafColumn = graph.leafMetadata.map(meta => (None, meta.distinct))
      (Seq((None, 1)), leafColumn.toSeq)
    } else {
      val (dlColumns, leafColumns) = (for {
        (step, (probability, subgraph)) <- graph.children.toSeq
        if probability > 0
       } yield appendStep(subgraph, step, probability)).unzip
      (dlColumns.flatten, leafColumns.flatten)
    }
  }

  private def appendStep(graph: RSIGraph, step: String, probability: Double) = {
    val (dlColumns, leafColumns) = colsFromRSIGraph(graph)
    val change = if (probability < 1) 1 else 0
    val appendedLeafColumns
      = leafColumns.map{case (path, distinct) => (step +: path, distinct)}
    val appendedDlColumns
      = dlColumns.map{case (path, distinct) => (step +: path, distinct + change)}
    (appendedDlColumns, appendedLeafColumns)
  }
}
