package de.unikl.cs.dbis.waves.util

/**
  * A trait to represent the (estimated) metadata of a column
  */
trait ColumnMetadata {

  /**
    * @return the number of distinct values in this column
    */
  def distinct: Long

  /**
    * @return The GiniCoefficient of the data in this column
    */
  def gini: Double

  /**
    * The separator is the highest value that would go into the left bucket
    * after a split
    *
    * @param quantile the percantage of values to go in the left bucket
    * @return the separator
    */
  def separator(quantile: Double = .5): ColumnValue

  /**
    * Calculate the resulting metadata after a split by the given quantile
    *
    * @param quantile the quantile of values to end up in the first partition
    * @return the metadata after the split or an error message
    */
  def split(quantile: Double = .5): Either[String,(ColumnMetadata, ColumnMetadata)]
}
