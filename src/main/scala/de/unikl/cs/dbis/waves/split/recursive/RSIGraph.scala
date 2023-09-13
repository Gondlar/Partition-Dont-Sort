package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.util.ColumnValue
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
  children: Map[String, (Double, RSIGraph)] = Map.empty,
  leafMetadata: Option[ColumnMetadata] = None
) {
  assert(children.values.map(_._1).forall(p => p >= 0 && p <= 1))
  assert(!leafMetadata.isDefined || children.isEmpty)

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
    * @param withMetadata if this parameter is true, this method only returns
    *                     true iff the leaf has assciated metadata. By default,
    *                     it is false.
    * @return true iff the path is a leaf
    */
  def isLeaf(path: PathKey, withMetadata: Boolean = false): Boolean = children.get(path.head) match {
    case None => false
    case Some((_, child)) => {
      if (path.isNested) {
        child.isLeaf(path.tail, withMetadata)
      } else child.children.isEmpty && (!withMetadata || child.leafMetadata.isDefined)
    }
  }

  /**
    * Set the metadata of the leaf at the given path to the given value. 
    *
    * @param path the path to set
    * @param metadata the metadata to set
    * @return the updated RSIGraph or a String describing the error
    */
  def setMetadata(path: Option[PathKey], metadata: ColumnMetadata): Either[String,RSIGraph]
    = path.map{ p => 
        val step = p.head
        for { childTuple <- children.get(step).toRight(s"$p is not a valid path")
              (probability, child) = childTuple
              updated <- child.setMetadata(path.tail, metadata)
        } yield copy(children = children.updated(step, (probability, updated)))
      }.getOrElse(
        if (children.isEmpty) Right(copy(leafMetadata = Some(metadata)))
        else Left("path is not a leaf")
      )

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
    * Calculate given quantile of the colum at the given root-to-leaf path.
    *
    * @param path the path to check
    * @param quantile the quantile of values from the column, defaults to the median
    * @return The value or an error if the path does not lead to a leaf with metadata
    */
  def separatorForLeaf(path: Option[PathKey], quantile: Double = .5): Either[String,ColumnValue]
    = path match {
      case None => leafMetadata
        .toRight("no metadata available")
        .map(_.separator(quantile))
      case Some(value) => children.get(path.head)
        .toRight("Leaf not found")
        .flatMap({ case (_, child) => child.separatorForLeaf(path.tail, quantile)})
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
  def splitBy(path: PathKey) : Either[String,(RSIGraph, RSIGraph)] = {
    val step = path.head
    if (!path.isNested) {
      if (isCertain(path)) Left("cannot split on certian paths") else {
        val absentSplit = copy(children = children.updated(step, children(step).copy(_1 = 0d)))
        val presentSplit = copy(children = children + ((step, (1d, children(step)._2))))
        Right(absentSplit, presentSplit)
      }
    } else {
      children(step)._2.splitBy(path.tail).map{ case (absent, present) =>
        val absentSplit = copy(children = children + ((step, (children(step)._1, absent))))
        val presentSplit = copy(children = children + ((step, (1d, present))))
        (absentSplit, presentSplit)
      }
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
    * @return (trueSplit, falseSplit) or an error if the quantile is outside the
    *         specified range or leaf is not an existing leaf of this RSIGraph
    */
  def splitBy(leaf: PathKey, quantile: Double)
    = if (quantile <= 0 || quantile >= 1) Left("0 < quantile < 1 must hold")
      else if (absoluteProbability(leaf) == 0) Left(s"$leaf is always absent")
      else if (!isLeaf(leaf, true)) Left(s"$leaf is not a leaf with metadata") 
      else splitByHelper(Some(leaf), quantile)

  private def splitByHelper(leaf: Option[PathKey], quantile: Double): Either[String,(RSIGraph, RSIGraph)] = {
    if (leaf.isDefined) {
      val step = leaf.head
      val (probability, subtree) = children(step)
      val removedFraction = absoluteProbability(leaf.get) * quantile
      val newProbability = (probability-removedFraction)/(1-removedFraction)
      
      subtree.splitByHelper(leaf.tail, quantile).map{ case (trueSide, falseSide) =>
        val trueSplit = copy(children = children.updated(step, (1d, trueSide)))
        val falseSplit = copy(children = children.updated(step, (newProbability, falseSide)))
        (trueSplit, falseSplit)
      }
    } else {
      leafMetadata.get.split(quantile).map{ case (trueMetadata, falseMetadata) =>
        (copy(leafMetadata = Some(trueMetadata)), copy(leafMetadata = Some(falseMetadata)))
      }
    }
  }

  /**
    * Calculate sum of gini indexes of all columns in this RSIGraph
    *
    * @return the gini index
    */
  def gini: Double = {
    val (count, g, dataColumnGini) = gini(1)
    count - g + dataColumnGini
  }

  private def gini(baseProbability: Double): (Int, Double, Double) = {
    // if this is a leaf, the base probability is the event probability
    if (children.isEmpty) {
      val prob = baseProbability*baseProbability
      val dataColumGini = leafMetadata
        .filter(_ => baseProbability > 0)
        .map(_.gini)
        .getOrElse(0d)
      return (1, prob, dataColumGini)
    }

    var count = 0
    var sum = 0d
    var dataColumnGini = 0d
    for((conditionalProbability, child) <- children.values) {
      // sum all squared probabilities if this node is present
      val presentProbability = conditionalProbability * baseProbability
      val (childCount, childGini, childDataGini) = child.gini(presentProbability)
      sum += childGini
      dataColumnGini += childDataGini
      
      // for each leaf under this child, add the absent probability once
      val absentProbability = (1-conditionalProbability) * baseProbability
      sum += childCount * (absentProbability) * (absentProbability)
      count += childCount
    }
    (count, sum, dataColumnGini)
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
