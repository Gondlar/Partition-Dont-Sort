package de.unikl.cs.dbis.waves.util

/**
  * Supertype for datastructures which keep information about the structure of a
  * document collection.
  */
trait StructuralMetadata {

  /**
    * Check whether the presence of the object at a given path is certain, i.e.,
    * whether we already know that it will be missing or present respectively.
    * Paths that are not part of this schema are certain because we know they
    * are absent
    *
    * @param path the path to check
    * @return true iff the path is certain
    */
  def isCertain(path: PathKey): Boolean

  /**
    * Check whether the given path refers to a leaf in this RSIGaph. 
    * Non-existant nodes are not considered leafs!
    *
    * @param path the path to check
    * @param withMetadata if this parameter is true, this method only returns
    *                     true iff the leaf has assciated metadata. By default,
    *                     it is false.
    * @return true iff the path is a leaf
    */
  def isLeaf(path: PathKey, withMetadata: Boolean = false): Boolean

  /**
    * Set the metadata of the leaf at the given path to the given value. 
    *
    * @param path the path to set
    * @param metadata the metadata to set
    * @return the updated VersionTree or a String describing the error
    */
  def setMetadata(path: Option[PathKey], metadata: ColumnMetadata): Either[String,StructuralMetadata]

  /**
    * Calculate the probability that the object referenced by the given path is
    * present. As opposed to the conditional probabilities stored in the tree,
    * this retuns the absolute probability.
    * Paths outside of the schema have a probability of 0.
    *
    * @param path the path to calculate
    * @return the absolute probability
    */
  def absoluteProbability(path: Option[PathKey]): Double

  /**
    * Shorthand for non-root paths
    * @see [[absoluteProbability(path:Option[PathKey])]]
    */
  final def absoluteProbability(path: PathKey): Double = absoluteProbability(Some(path))

  /**
    * Calculate given quantile of the colum at the given root-to-leaf path.
    * 
    * The quantile is a request that may not be able to be matched exactly.
    * Instead, the method returns the actual probability of being less than or
    * equal to the given separator along with the value.
    *
    * @param path the path to check
    * @param quantile the quantile of values from the column, defaults to the median
    * @return The value and its actual quantile or an error if the path does not
    *         lead to a leaf with metadata
    */
  def separatorForLeaf(path: Option[PathKey], quantile: Double = .5): Either[String,(ColumnValue, Double)]

  /**
    * Check whether the path is a valid split location, i.e., it is not certain
    * and its absolute probability is greater than zero
    *
    * @param path the path to check
    * @return true iff the path is a valid split location
    */
  def isValidSplitLocation(path: PathKey): Boolean
    = !isCertain(path) && absoluteProbability(path) != 0

  /**
    * Given a non-certain path, determine the VersionTrees resulting from splitting
    * based on the presence of that path. We assume that the presence of all
    * objects that are not a prefix of each other is independant.
    *
    * @param path a non-certain path to split by
    * @return a tuple of two VersionTrees: (absent, present)
    */
  def splitBy(path: PathKey) : Either[String,(StructuralMetadata, StructuralMetadata)]

  /**
    * Given a root-to-leaf path with higher-than-zero probability of existing, 
    * determine the VersionTrees resulting from splitting a percentage of values
    * into their own bucket. All Null-values go to the remaining bucket. We
    * assume that the presence of all objects that are not a prefix of each
    * other is independant.
    *
    * @param leaf the path to the leaf whose values are to be split in their own
    *             bucket
    * @param quantile the precentage of existing values that is split off. As
    *                 such, 0 < quantile < 1 must hold.
    * @return (trueSplit, falseSplit) or an error if the quantile is outside the
    *         specified range or leaf is not an existing leaf of this VersionTree
    */
  def splitBy(leaf: Option[PathKey], quantile: Double): Either[String,(StructuralMetadata, StructuralMetadata)]
  
  /**
    * Shorthand for existing paths
    * @see [[splitBy(leaf: Option[PathKey],quantile:Double)]]
    */
  def splitBy(leaf: PathKey, quantile: Double): Either[String,(StructuralMetadata, StructuralMetadata)]
    = splitBy(Some(leaf), quantile)

  def gini: Double
}
