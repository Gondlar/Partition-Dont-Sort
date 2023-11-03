package de.unikl.cs.dbis.waves.pipeline.sample

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/**
  * Take the first n rows of each partition as a sample or entire partitions if
  * they contain fewer than n rows.
  * 
  * This abandons any statistical properties and can introduce significant bias
  * into the calculation, especially if the data is sorted in some manner.
  * However, it may also be effective in reducing the runtime in cases where the
  * bottleneck is before the sampling, e.g. JSON parsing. In those cases, uniform
  * sampling still has to parse the entire document, but for this method can stop
  * parsing after n entries.
  * 
  * It may also capture properties that specifically result from the order, e.g.,
  * for compression estimations as used in BtrBlocks.
  *
  * @param count the number of rows to take from each partition
  */
final case class InitSample(
  count: Int
) extends Sampler {

  override def apply(df: DataFrame): DataFrame = {
    implicit val encoder = RowEncoder(df.schema)
    df.mapPartitions(p => p.take(count))
  }

  override def explain = s"Init $count"

}
