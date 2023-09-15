package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.partitions.Partitioned
import de.unikl.cs.dbis.waves.util.operators.findBucket

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

/**
  * This pipeline step derives Buckets from Shape. Given a state where Shape is
  * set, it sets Buckets such that its contents reflect the splits done in the
  * Shape tree.
  */
object ShuffleByShape extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = Shape isDefinedIn state

  override def run(state: PipelineState): PipelineState =  {
    val shape = Shape(state)
    val shuffledDf = state.data.repartition(
      shape.buckets.size,
      findBucket(shape.indexes, Schema(state))
    )
    state.copy(data = shuffledDf)
  }

  private def makeFilter(df: DataFrame, metadata: PartitionMetadata) = {
    val absent = metadata.getAbsent.map(k => k.toCol(df).isNull)
    val present = metadata.getPresent.map(k => k.toCol(df).isNotNull)
    (absent ++ present).reduce((lhs, rhs) => lhs && rhs)
  }
}
