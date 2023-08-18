package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructField

/**
  * This pipeline step modifies the schema of each bucket such that it reflects
  * the metadata known about it from the partiton tree shape. More specifically,
  * it removes all nodes known to be absent and marks all known present
  * nodes as required. As a result, each partition folder will have a different
  * local schema.
  */
object SchemaModifier extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = (Buckets isDefinedIn state) && (Shape isDefinedIn state)

  override def run(state: PipelineState): PipelineState = {
    val known = KnownMetadata(state)
    val allMetadata = Shape(state).metadata().map(known ++ _)
    val buckets = Buckets(state)

    val modified = for ((bucket, metadata) <- buckets.zip(allMetadata)) yield {
      // drop all known absent columns
      val dropped = metadata.getAbsent.foldLeft(bucket)((df, path) => dropNested(path, df))
      // mark all known present columns as such and apply the new schema
      val schema = metadata.getPresent.foldLeft(dropped.schema)((schema, path) => schema.withPresent(path))
      dropped.sparkSession.createDataFrame(dropped.rdd, schema)
    }

    Buckets(state) = modified
  }

  /**
    * Spark's dropColumn function can only drop non-nested columns, so we need
    * our own.
    *
    * @param key the nested column we want to drop
    * @param df the dataframe
    */
  private def dropNested(key: PathKey, df: DataFrame) = {
    if (key.isNested) {
      val cols = df.schema.fields.map({case StructField(name, _, _, _) =>
        if (name == key.head) col(name).dropFields(key.tail.toSpark).as(name)
        else col(name)
      })
      df.select(cols:_*)
    } else df.drop(key.toCol)
  }
}
