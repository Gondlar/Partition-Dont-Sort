package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.partitions.Partitioned
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import de.unikl.cs.dbis.waves.partitions.NWayPath

/**
  * This pipeline step derives Buckets from Shape. Given a state where Shape is
  * set, it sets Buckets such that its contents reflect the splits done in the
  * Shape tree.
  */
object BucketsFromShape extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = Shape isDefinedIn state

  override def run(state: PipelineState): PipelineState =  {
    val df = state.data
    val spark = df.sparkSession
    val metadata = Shape(state).metadata()
    val emptdDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema(state))
    Buckets(state) = metadata.map({ metadata =>
      if (metadata.isSpillBucket) emptdDf
      else if (metadata.getPath.filter(step => step != Partitioned && !step.isInstanceOf[NWayPath]).isEmpty) df
      else df.filter(makeFilter(df, metadata))
    })
  }

  private def makeFilter(df: DataFrame, metadata: PartitionMetadata) = {
    val absent = metadata.getAbsent.map(k => k.toCol(df).isNull)
    val present = metadata.getPresent.map(k => k.toCol(df).isNotNull)
    (absent ++ present).reduce((lhs, rhs) => lhs && rhs)
  }
}
