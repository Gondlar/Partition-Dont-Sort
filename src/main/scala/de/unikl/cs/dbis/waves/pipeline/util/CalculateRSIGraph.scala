package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import de.unikl.cs.dbis.waves.split.recursive.PresentMetric
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph
import de.unikl.cs.dbis.waves.util.nested.schemas._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{
  col, sum, count, when, min, max, approx_count_distinct
}
import org.apache.spark.sql.types.StructType

/**
  * Set the StructureMetadata field of the Pipeline State to an RSIGraph
  * calculated from the state's data.
  */
object CalculateRSIGraph extends PipelineStep with NoPrerequisites {

  override def run(state: PipelineState): PipelineState = {
    val tree = dfToRSIGraph(state.data, Schema(state))
    StructureMetadata(state) = tree
  }

  /**
    * Construct a RSIGraph which holds the metadata of a given DataFrame and
    * schema. This method will perform two scans on the DataFrame
    *
    * @param df the DataFrame to process
    * @param schema the schema of the df
    * @return the constructed RSIGraph
    */
  def dfToRSIGraph(df: DataFrame, schema: StructType): RSIGraph = {
    val structureColumns = makeStructureColumns(schema)
    val metadataColumns = makeMetadataColumns(schema)
    val aggs = df.agg(count("*"), (structureColumns ++ metadataColumns):_*).head().toSeq
    val size = aggs(0).asInstanceOf[Long]
    val graph = loadRSIGRaph(size, aggs.slice(1, structureColumns.size+1).asInstanceOf[Seq[Long]], schema)
    addColumnMetadataToGraph(aggs.slice(1+structureColumns.size, aggs.size), graph, schema)
  }

  private def makeStructureColumns(schema: StructType)
    = schema.optionalPaths.map(p => count(when(p.toCol.isNotNull, 1)))

  private def loadRSIGRaph(count: Long, data: Iterable[Long], schema: StructType) = {
    val counter = new ObjectCounter(data.map(_.asInstanceOf[Long].toInt).toArray)
    RSIGraph.fromObjectCounter(counter, schema, count.toInt)
  }

  private def makeMetadataColumns(schema: StructType) = {
    val leafs = schema.leafPaths.map(_.toCol)
    val res = Seq.newBuilder[Column]
    res ++= leafs.iterator.map(c => min(c))
    res ++= leafs.iterator.map(c => max(c))
    res ++= leafs.iterator.map(c => approx_count_distinct(c))
    res.result()
  }

  private def addColumnMetadataToGraph(summary: Seq[Any], inputGraph: RSIGraph, schema: StructType) = {
    val leafs = schema.leafPaths
    val leafCount = leafs.size
    assert(summary.size == leafCount*3)
    var graph = inputGraph
    for {
      (leaf, minIndex) <- leafs.zipWithIndex
      maxIndex = minIndex+leafCount
      distinctIndex = minIndex+2*leafCount
      meta <- ColumnMetadata.fromSeq(summary, minIndex, maxIndex, distinctIndex)
    } {
      graph = graph.setMetadata(Some(leaf), meta).right.get
    }
    graph
  }
}
