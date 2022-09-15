package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  typedLit, collect_list, map_from_entries, struct, col, monotonically_increasing_id, sum
}
import org.apache.spark.sql.types.StructType

/**
  * Groupers transform a source data frame into a grouped representation and back
  */
trait Grouper {

  /**
    * Alias for [[group]]
    */
  def apply(data: DataFrame): DataFrame = group(data)

  /**
    * Given a source data frame, transform it into the grouped representation
    * The schema of the grouped representtion depends on the concrete
    * implementation of the grouper.
    *
    * @param data the source data
    * @return the grouped data
    */
  def group(data: DataFrame): DataFrame

  /**
    * Given a DataFrame in grouped representation and a DataFrame in the
    * source representation, find all Rows of the source data which belong to
    * one of the groups in the grouped data.
    *
    * @param bucket the groups
    * @param data the source data
    * @return all entries from source data that belong to the groups from bucket
    */
  def find(bucket: DataFrame, data: DataFrame): DataFrame

  /**
    * Given a sequence of disjoint DataFrames containing the grouped
    * representation and a DataFrame containing the source representation,
    * identify which DataFrame each Row of source data corresponds to. To this
    * end append an additional column to the source data which holds the 
    * corresponding grouped DataFrame's index in the given list. To find that 
    * column's name, see [[partitionColumn]].
    *
    * @param buckets the grouped represenatations
    * @param data the source data. It must have the same schema as used to 
    *             create the grouped entries in the buckets.
    * @return data with an additional column.
    */
  def matchAll(buckets: Seq[DataFrame], data: DataFrame): DataFrame

  /**
    * @return The additional column appended by [[matchAll]]
    */
  def matchColumn: TempColumn

  /**
    * Sort the given source data such that it is orderd in the same order as the
    * corresponding groups in the given grouped DataFrame.
    *
    * @param bucket the "sort order"
    * @param data The data to sort. It must have the same schema as used to
    *             create the grouped entries in bucket.
    * @return data, but sorted
    */
  def sort(bucket: DataFrame, data: DataFrame): DataFrame

  /**
    * Given a data frame which follows the groupers grouped schema, determine
    * how many original data rows the contained groups represent
    *
    * @param df the data frame
    * @return the count
    */
  def count(df: DataFrame): Long

  /**
    * Convert data grouped by another grouper to the grouped format used by this
    * grouped.
    * 
    * The default implementation only handles the special case where other is
    * equal to this. Otherwise, it transforms the data by [[find]]ing the data
    * in the bucket using the old grouper and then [[group]]ing it again using
    * this grouper. Groupers can override this method to provide more specialized
    * transformations.
    *
    * @param other the other grouper
    * @param bucket data grouped by other
    * @param data the source data for the bucket
    * @return the groups in bucket according to this grouper
    */
  def from(other: Grouper, bucket: DataFrame, data: DataFrame)
    = if (other == this) bucket else group(other.find(bucket, data))
}
