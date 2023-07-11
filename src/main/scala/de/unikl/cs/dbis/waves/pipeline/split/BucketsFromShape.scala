package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

/**
  * This pipeline step derives Buckets from Shape. Given a state where Shape is
  * set, it sets Buckets such that its contents reflect the splits done in the
  * Shape tree.
  */
object BucketsFromShape extends PipelineStep {

  override def isSupported(state: PipelineState): Boolean
    = Shape.isDefined(state)

  override def run(state: PipelineState): PipelineState =  {
    val df = state.data
    val spark = df.sparkSession
    val metadata = Shape(state).metadata()
    val emptdDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema(state))
    Buckets(state) = if (metadata.length > 1) {
      metadata.map({ metadata =>
        if (metadata.isSpillBucket) emptdDf
        else df.filter(makeFilter(df, metadata))
      })
    } else Seq(state.data)
  }

  private def makeFilter(df: DataFrame, metadata: PartitionMetadata) = {
    val absent = metadata.getAbsent.map(k => df.col(k.toSpark).isNull)
    val present = metadata.getPresent.map(k => df.col(k.toSpark).isNotNull)
    (absent ++ present).reduce((lhs, rhs) => lhs && rhs)
  }
}
