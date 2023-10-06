package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.{VersionTree, Leaf, Versions}

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

object VersionTreeCardinalities extends ColumnOrderer {

  override def supports(state: PipelineState): Boolean
    = StructureMetadata isDefinedIn state

  override def sort(state: PipelineState, df: DataFrame): Seq[Column] = {
    val (dlColumns, columns) = colsFromVersionTreeGraph(StructureMetadata(state))
    val spark1 = dlColumns.map{case (path, distinct) => (ExactCardinalities.definitionLevel(path.get), distinct)}
    val spark2 = columns.map{case (path, distinct) => (path.get.toCol, distinct)}
    (spark1 ++ spark2).filter(_._2 > 1).sortBy(_._2).map(_._1)
  }

  def colsFromVersionTreeGraph(graph: VersionTree): (Seq[(Option[PathKey], Long)], Seq[(Option[PathKey], Long)])
    = graph match {
      case Leaf(metadata) => {
        val leafColumn = metadata.map(meta => (None, meta.distinct))
        (Seq((None, 1)), leafColumn.toSeq)
      }
      case v@Versions(_, _, _) => {
        val (dlColumns, leafColumns) = (for {
          (step, probability, subgraph) <- v.childIterator
          if probability > 0
        } yield appendStep(subgraph, step, probability)).toSeq.unzip
        (dlColumns.flatten, leafColumns.flatten)
      }
    }

  private def appendStep(graph: VersionTree, step: String, probability: Double) = {
    val (dlColumns, leafColumns) = colsFromVersionTreeGraph(graph)
    val change = if (probability < 1) 1 else 0
    val appendedLeafColumns
      = leafColumns.map{case (path, distinct) => (step +: path, distinct)}
    val appendedDlColumns
      = dlColumns.map{case (path, distinct) => (step +: path, distinct + change)}
    (appendedDlColumns, appendedLeafColumns)
  }
}
