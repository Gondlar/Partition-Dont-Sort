package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.operators.findBucket
import de.unikl.cs.dbis.waves.util.operators.TempColumn

/**
  * This pipeline step derives Partitions from Shape. Given a state where Shape
  * is set, it sets the shuffle column such that each Bucket has a unique ID in
  * it. This can be used to shuffle the data according to the later partitioning.
  */
object ShuffleByShape extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = (Shape isDefinedIn state) && !(ShuffleColumn isDefinedIn state)

  override def run(state: PipelineState): PipelineState = {
    val shuffleColumn = TempColumn("shuffleColumn")
    val newDf = state.data.withColumn(shuffleColumn, findBucket(Shape(state).indexes, Schema(state)))
    (ShuffleColumn(state) = shuffleColumn).copy(data = newDf)
  }
}
