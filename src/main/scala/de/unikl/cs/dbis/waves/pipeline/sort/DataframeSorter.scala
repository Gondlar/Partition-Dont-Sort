package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.pipeline._
import org.apache.spark.sql.DataFrame

/**
  * Sort the Buckets of a Pipeline lexicographically.
  * To this end, the Buckets as well as a Global or Bucket Sortorder must be set.
  */
object DataframeSorter extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = Buckets.isDefined(state) && (GlobalSortorder.isDefined(state) || BucketSortorders.isDefined(state))

  override def run(state: PipelineState): PipelineState = {
    val buckets = Buckets(state)
    val orders = BucketSortorders.get(state)
      .map(_.iterator)
      .getOrElse(Iterator.fill(buckets.size)(GlobalSortorder(state)))
    val sortedBuckets = buckets.iterator.zip(orders).map({ case (df, order) =>
      df.sort(order:_*)
    }).toSeq
    Buckets(state) = sortedBuckets
  }
}
