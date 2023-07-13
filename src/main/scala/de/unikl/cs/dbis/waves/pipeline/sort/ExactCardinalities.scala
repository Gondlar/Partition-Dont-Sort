package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count_distinct}
import org.apache.spark.sql.types.IntegerType

object ExactCardinalities extends ColumnOrderer {

  override def supports(state: PipelineState): Boolean = true

  override def sort(state: PipelineState, df: DataFrame): Seq[Column] = {
    // generate columns for all value and definition level columns
    val paths = df.schema.leafPaths
    val cols = (paths.map(p => col(p.toSpark)) ++ paths.map(definitionLevel(_)))

    // get all cardinalities
    val withCount = cols.map(count_distinct(_))
    val cardinalities = df.agg(withCount.head, withCount.tail:_*).head()

    // order in increasing order ignoring those with card 1
    cols.indices.map(cardinalities.getLong(_)).zip(cols) // map cardinalities to columns
      .filter(_._1 > 1)                                  // ignore cardinality 1 columns
      .sortBy(_._1)                                      // sort by cardinality
      .map(_._2)                                         // get the ordered columns
  }

  def definitionLevel(path: PathKey) = {
    var current = path
    val builder = Seq.newBuilder[PathKey]
    builder += path
    while (current.isNested) {
      current = current.parent
      builder += current
    }
    builder.result().map(step => col(step.toSpark).isNotNull.cast(IntegerType)).reduce(_+_)
  }
}
