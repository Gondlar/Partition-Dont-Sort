package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.util.PathKey
import org.apache.spark.sql.types.StructType

/**
  * Inspired by Klettke et al.'s Reduced Structure Identification Graph, the
  * Relative Structure Identification Graph (RSIGraph) stores the conditional
  * probability of a node given nodes on its path to the root are present
  *
  * @param children
  */
final case class RSIGraph(
  children: Map[String, (Double, RSIGraph)] = Map.empty
) {
  assert(children.values.map(_._1).forall(p => p >= 0 && p <= 1))

  /**
    * Check whether the presence of the object at a given path is certain, i.e.,
    * whether we already know that it will be missing or present respectively.
    * Paths that are not part of this schema are certain because we know they
    * are absent
    *
    * @param path the path to check
    * @return true iff the path is certain
    */
  def isCertain(path: PathKey) : Boolean = {
    val option = for((prob, child) <- children.get(path.head)) yield {
      if (path.isNested) child.isCertain(path.tail)
      else prob == 1 || prob == 0
    }
    option.getOrElse(true)
  }

  /**
    * Check whether the given path refers to a leaf in this RSIGaph. 
    * Non-existant nodes are not considered leafs!
    *
    * @param path the path to check
    * @return true iff the path is a leaf
    */
  def isLeaf(path: PathKey): Boolean = children.get(path.head) match {
    case None => false
    case Some((_, child)) => {
      if (path.isNested) {
        child.isLeaf(path.tail)
      } else child.children.isEmpty
    }
  }

  /**
    * Calculate the probability that the object referenced by the given path is
    * present. As opposed to the conditional probabilities stored in the tree,
    * this retuns the absolute probability.
    * Paths outside of the schema have a probability of 0.
    *
    * @param path the path to calculate
    * @return the absolute probability
    */
  def absoluteProbability(path: PathKey): Double
    = children.get(path.head) match {
      case None => 0
      case Some((prob, child)) if path.isNested =>
          prob * child.absoluteProbability(path.tail)
      case Some((prob, _)) => prob
    }

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
    * Given a non-certain path, determine the RSIGraphs resulting from splitting
    * based on the presence of that path. We assume that the presence of all
    * objects that are not a prefix of each other is independant.
    *
    * @param path a non-certain path to split by
    * @return a tuple of two RSIGraphs: (absent, present)
    */
  def splitBy(path: PathKey) : (RSIGraph, RSIGraph) = {
    val step = path.head
    if (!path.isNested) {
      require(!isCertain(path))
      val absentSplit = RSIGraph(children.updated(step, children(step).copy(_1 = 0d)))
      val presentSplit = RSIGraph(children + ((step, (1d, children(step)._2))))
      (absentSplit, presentSplit)
    } else {
      val (absent, present) = children(step)._2.splitBy(path.tail)
      val absentSplit = RSIGraph(children + ((step, (children(step)._1, absent))))
      val presentSplit = RSIGraph(children + ((step, (1d, present))))
      (absentSplit, presentSplit)
    }
  }

  /**
    * Given a root-to-leaf path with higher-than-zero probability of existing, 
    * determine the RSIGraphs resulting from splitting a percentage of values
    * into their own bucket. All Null-values go to the remaining bucket. We
    * assume that the presence of all objects that are not a prefix of each
    * other is independant.
    *
    * @param leaf the path to the leaf whose values are to be split in their own
    *             bucket
    * @param quantile the precentage of existing values that is split off. As
    *                 such, 0 < quantile < 1 must hold.
    * @return (trueSplit, falseSplit)
    * @throws IllegalArgumentException if the quantile is outside the specified
    *                                  range or leaf is not an existing leaf of
    *                                  this RSIGraph
    */
  def splitBy(leaf: PathKey, quantile: Double) = {
    require(quantile > 0 && quantile < 1, "0 < quantile < 1")
    require(absoluteProbability(leaf) > 0, s"$leaf is always absent")
    require(isLeaf(leaf), s"$leaf is not a leaf")
    splitByHelper(Some(leaf), quantile)
  }

  private def splitByHelper(leaf: Option[PathKey], quantile: Double): (RSIGraph, RSIGraph) = {
    if (leaf.isDefined) {
      val step = leaf.head
      val (probability, subtree) = children(step)
      val removedFraction = absoluteProbability(leaf.get) * quantile
      val newProbability = (probability-removedFraction)/(1-removedFraction)
      
      val (trueSide, falseSide) = subtree.splitByHelper(leaf.tail, quantile)
      val trueSplit = RSIGraph(children.updated(step, (1d, trueSide)))
      val falseSplit = RSIGraph(children.updated(step, (newProbability, falseSide)))
      (trueSplit, falseSplit)
    } else (this, this)
  }

  /**
    * Calculate sum of gini indexes of all columns in this RSIGraph
    *
    * @return the gini index
    */
  def gini: Double = {
    val (count, g) = gini(1)
    count - g
  }

  private def gini(baseProbability: Double): (Int, Double) = {
    // if this is a leaf, the base probability is the event probability
    if (children.isEmpty) return (1, baseProbability*baseProbability)

    var count = 0
    var sum = 0d
    for((conditionalProbability, child) <- children.values) {
      // sum all squared probabilities if this node is present
      val presentProbability = conditionalProbability * baseProbability
      val (childCount, childGini) = child.gini(presentProbability)
      sum += childGini
      
      // for each leaf under this child, add the absent probability once
      val absentProbability = (1-conditionalProbability) * baseProbability
      sum += childCount * (absentProbability) * (absentProbability)
      count += childCount
    }
    (count, sum)
  }
}

object RSIGraph {

  /**
    * Create an RSIGraph from a given list of children and weights.
    * 
    * This method mainly provides syntactic sugar for testing purposes
    *
    * @param children
    * @return
    */
  def apply(children: (String, Double, RSIGraph)*) : RSIGraph
    = RSIGraph(Map(children.map({ case (name, prob, child) =>
      (name, (prob, child))
    }):_*))

  /**
    * An empty RSIGraph
    */
  val empty = RSIGraph()
  
  /**
    * Create an RSIGraph from an ObjectCounter
    *
    * @param counter the counter containing the absolute counts of all optional objects in a datasets
    * @param schema the schema corresponding to the object counter
    * @param total the size of the dataset
    * @return the RSIGraph with the conditional probabilities reflecting the frequencies from the counter
    */
  def fromObjectCounter(counter: ObjectCounter, schema: StructType, total: Int) = {
    val map = counter.toMap(ObjectCounter.paths(schema))
      .map({ case (key, count) => (Some(key): Option[PathKey], count)})
      .updated(None, total)
    fromObjectCounterHelper(map, schema, None)
  }

  private def fromObjectCounterHelper(map: Map[Option[PathKey], Int], schema: StructType, path: Option[PathKey]): RSIGraph = {
    val count = map(path)
    val children = for(field <- schema.fields) yield {
      val subpath = path :+ field.name
      val subtree = field.dataType match {
        case struct : StructType => fromObjectCounterHelper(map, struct, subpath)
        case _ => RSIGraph.empty
      }
      val conditional = if (field.nullable) relative(count, map(subpath)) else 1
      (field.name, (conditional, subtree))
    }
    RSIGraph(children.toMap)
  }

  private def relative(total: Double, partial: Double)
    = if (total == 0) 0 else partial/total
}
