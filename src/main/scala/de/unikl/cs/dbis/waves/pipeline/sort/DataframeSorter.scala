package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline.PipelineStep
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.pipeline.Buckets
import de.unikl.cs.dbis.waves.pipeline.GlobalSortorder
import de.unikl.cs.dbis.waves.pipeline.BucketSortorders
import org.apache.spark.sql.DataFrame

object DataframeSorter extends PipelineStep {

  override def isSupported(state: PipelineState): Boolean
    = Buckets.isDefined(state) && (GlobalSortorder.isDefined(state) || BucketSortorders.isDefined(state))

  override def run(state: PipelineState): PipelineState = {
    val buckets = Buckets(state).iterator
    val orders = BucketSortorders.get(state)
      .map(_.iterator)
      .getOrElse(Iterator.fill(buckets.size)(GlobalSortorder(state)))
    val sortedBuckets = buckets.zip(orders).map({ case (df, order) =>
      df.sort(order:_*)
    }).toSeq
    Buckets(state) = sortedBuckets
  }
}
