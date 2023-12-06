package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.operators.TempColumn

import org.apache.spark.sql.functions.spark_partition_id

/**
  * This pipeline step derives a shuffle column from the current partitioning
  * of a data frame
  */
object Preshuffled extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = !(ShuffleColumn isDefinedIn state)

  override def run(state: PipelineState): PipelineState = {
    val shuffleColumn = TempColumn("shuffleColumn")
    val newDf = state.data.withColumn(shuffleColumn, spark_partition_id)
    (ShuffleColumn(state) = shuffleColumn).copy(data = newDf)
  }
}
