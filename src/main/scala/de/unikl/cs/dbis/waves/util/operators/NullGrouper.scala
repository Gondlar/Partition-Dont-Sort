package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.typedLit

/**
  * The NullGrouper is used in cases where no actual grouping happens.
  * The grouped representation is identical to the data.
  *
  * @param GROUP_COLUMN the name of the Column containing the group information
  */
object NullGrouper extends Grouper {
    
  val MATCH_COLUMN = TempColumn("partition")

  // Grouped representation is identical to the source data
  override def group(data: DataFrame): DataFrame = data

  // Since we assume all groups from bucket are present in data and our groups
  // are just Rows, we can just return bucket
  override def find(bucket: DataFrame, data: DataFrame) = bucket

  // Since we assume all groups from bucket are present in data and our groups
  // are just Rows, we can just return bucket
  override def sort(bucket: DataFrame, data: DataFrame) = bucket.repartition(1)

  // All groups are individual rows
  override def count(df: DataFrame) = df.count()
  override def matchColumn: TempColumn = MATCH_COLUMN

  // This is the only function in this grouper that actually does something
  override def matchAll(buckets: Seq[DataFrame], data: DataFrame) = {
    buckets.zipWithIndex
           .map({case (b, i) => b.withColumn(matchColumn, typedLit(i))})
           .reduce(_.union(_))
  }
}
