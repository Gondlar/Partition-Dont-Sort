package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._

import org.apache.spark.sql.functions.col

/**
  * This pipeline step shuffles the data according to the shuffle column set in
  * the state. This is useful for paralell operations such as writing and sorting.
  */
object Shuffle extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = (ShuffleColumn isDefinedIn state) && (NumBuckets isDefinedIn state)

  override def run(state: PipelineState): PipelineState =  {
    val shuffledDf = state.data.repartition(
      NumBuckets(state),
      col(ShuffleColumn(state)) + 1 //there is a hash collision between 0 and 1 we'd always hit
    )
    state.copy(data = shuffledDf)
  }
}
