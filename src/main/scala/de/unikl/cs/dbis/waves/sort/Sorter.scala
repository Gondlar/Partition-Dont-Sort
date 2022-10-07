package de.unikl.cs.dbis.waves.sort

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.util.operators.Grouper

trait Sorter {

  /**
    * Sort the data within a bucket
    *
    * @param bucket the bucket grouped by [[sortGrouper]]
    * @return the sorted bucket
    */
  def sort(bucket: DataFrame): DataFrame

  /**
    * A Grouper to group the data by. Each grouping represents one kind of data
    * This grouper is used when sorting the data.
    */
  def grouper: Grouper
}
