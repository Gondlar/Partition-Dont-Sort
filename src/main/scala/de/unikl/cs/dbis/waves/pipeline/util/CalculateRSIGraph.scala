package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import de.unikl.cs.dbis.waves.split.recursive.PresentMetric
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph
import de.unikl.cs.dbis.waves.util.nested.schemas._

import org.apache.spark.sql.DataFrame
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
  def dfToRSIGraph(df: DataFrame, schema: StructType): RSIGraph
    = addColumnMetadataToGraph(df,schema,loadRSIGRaph(df, schema))

  private def loadRSIGRaph(df: DataFrame, schema: StructType) = {
    val optionalCount = schema.optionalNodeCount()
    val (rows, counts) = df.rdd.mapPartitions({ partition => 
      val totalCount = ObjectCounter(optionalCount)
      val currentCount = ObjectCounter(optionalCount)
      var rowCount = 0
      for(row <- partition) {
        rowCount += 1
        currentCount <-- PresentMetric(row)
        totalCount += currentCount
      }
      Iterator((rowCount, totalCount))
    }).reduce({ case ((rowsLhs, countsLhs), (rowsRhs, countRhs)) =>
      countsLhs += countRhs
      (rowsLhs + rowsRhs, countsLhs)
    })
    RSIGraph.fromObjectCounter(counts, schema, rows)
  }

  private def addColumnMetadataToGraph(df: DataFrame, schema: StructType, inputGraph: RSIGraph) = {
    val leafs = schema.leafPaths
    val aggs = leafs.map(p => (p.toDotfreeString, "min")) ++
      leafs.map(p => (p.toDotfreeString, "max")) ++
      leafs.map(p => (p.toDotfreeString, "approx_count_distinct"))
    val summary = df.select(leafs.map(p => p.toCol.as(p.toDotfreeString)):_*)
      .agg(aggs.head, aggs.tail:_*)
      .head()
    var graph = inputGraph
    for {
      (leaf, minIndex) <- leafs.zipWithIndex
      maxIndex = minIndex+leafs.length
      distinctIndex = minIndex+2*leafs.length
      meta <- ColumnMetadata.fromRows(summary, minIndex, maxIndex, distinctIndex)
    } {
      graph = graph.setMetadata(Some(leaf), meta).right.get
    }
    graph
  }
}
