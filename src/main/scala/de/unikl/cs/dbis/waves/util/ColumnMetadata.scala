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
    * Calulate the probability of a value in this colum to be less than or equal
    * to the given value. It must be of the correct type.
    *
    * @param separator the value
    * @return P(x <= separator) or None if the metadata cannot provide a good
    *         estimate
    */
  def probability(separator: ColumnValue): Option[Double]

  /**
    * Refine a given quantile for a split based on the metadta.
    * 
    * E.g., consider a Boolean column which has 80% true values. Even if we want
    * to split it by the median, only 20% of values will end up in the partition
    * of "false" values. If the metadata has ne better estimate, return the
    * quantile again
    *
    * @param quantile the input quantile
    * @return the refined quantile
    */
  def refinedProbability(quantile: Double = .5): Double
    = probability(separator(quantile)).getOrElse(quantile)

  /**
    * Calculate the resulting metadata after a split by the given quantile
    *
    * @param quantile the quantile of values to end up in the first partition
    * @return the metadata after the split or an error message
    */
  def split(quantile: Double = .5): Either[String,(ColumnMetadata, ColumnMetadata)]
}
