package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import de.unikl.cs.dbis.waves.util.StructuralMetadata
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import Math.min

/**
  * Split the data based on a VersionTree model of the data using the Gini Index
  * to choose the best splits.
  * The RSIGgraph must have been calculated beforehand.
  * 
  * @see [[CalculateVersionTree]] for how to calculate the VersionTree
  * @param maxBuckets
  * @param minimumBucketFill the minimum size - relative to an even distribution
  *                          - that a bucket may be expected to have for the
  *                          split to be considered. Defaults to 50%
  */
case class ModelGini(
  maxBucketSize: Double,
  minBucketSize: Double,
  useColumnSplits: Boolean
) extends Recursive[SplitCandidateState] {
  import ModelGini._

  assert(maxBucketSize > 0 && maxBucketSize <= 1)
  assert(minBucketSize > 0 && minBucketSize <= 1)
  assert(minBucketSize < maxBucketSize)

  private var splitLocations: RDD[SplitCandidate] = null

  override def supports(state: PipelineState): Boolean
    = StructureMetadata isDefinedIn state

  override protected def initialRecursionState(state: PipelineState): SplitCandidateState = {
    val schema = Schema(state)

    val spark = state.data.sparkSession
    splitLocations = spark.sparkContext.parallelize[SplitCandidate](
      schema.optionalPaths.map(PresenceSplitCandidate(_)) ++
      (if (useColumnSplits) schema.leafPaths.map(MedianSplitCandidate(_)) else Seq.empty)
    ).persist()
    
    SplitCandidateState(StructureMetadata(state), 1, Seq.empty)
  }

  override protected def checkRecursion(recState: SplitCandidateState): Boolean
    = recState.size > maxBucketSize

  override protected def doRecursionStep(recState: SplitCandidateState, df: DataFrame): (Seq[SplitCandidateState], DataFrame => TreeNode.AnyNode[DataFrame]) = {
    val split = findBestSplit(recState.graph, recState.path, recState.size).get  //TODO handle
    val leftSize = split.leftFraction(recState.graph)
    val (leftGraph, rightGraph) = split.split(recState.graph).right.get
    val (leftStep, rightStep) = split.paths
    val leftCandidate = SplitCandidateState(leftGraph, leftSize * recState.size, recState.path :+ leftStep)
    val rightCandidate = SplitCandidateState(rightGraph, (1- leftSize) * recState.size, recState.path :+ rightStep)
    (Seq(leftCandidate, rightCandidate).filter(_.size > maxBucketSize), df => split.shape(df, recState.graph))
  }

  private def findBestSplit(tree: StructuralMetadata, path: Seq[PartitionTreePath], size: Double): Option[SplitCandidate] = {
    splitLocations.mapPartitions({ partition =>
      val splits = for {
        candidate <- partition
        if candidate isValidFor tree
        leftFraction = candidate.leftFraction(tree)
        if minBucketSize <= min(leftFraction, 1-leftFraction)*size
        split = candidate.split(tree)
        if split.isRight
        (leftSide, rightSide) = split.right.get
      } yield {
        val gini = leftFraction * leftSide.gini + (1-leftFraction) * rightSide.gini
        (candidate, gini)
      }
      if (splits.isEmpty) Iterator.empty else Iterator(Some(splits.minBy(_._2)): Option[(SplitCandidate, Double)])
    }).fold(None)(mergeOptions({ case (lhs@(_, lhsGini), rhs@(_, rhsGini)) =>
      if (lhsGini < rhsGini) lhs else rhs
    })).map(_._1)
  }
}

object ModelGini {

  def apply(maxBucketSize: Double, useColumnSplits: Boolean = true): ModelGini
    = apply(maxBucketSize, maxBucketSize/2, useColumnSplits)

  def mergeOptions[A](fn: (A, A) => A)(lhs: Option[A], rhs: Option[A]): Option[A] = {
    if (lhs.isEmpty) return rhs
    if (rhs.isEmpty) return lhs
    Some(fn(lhs.get, rhs.get))
  }
}

sealed trait SplitCandidate {
  def isValidFor(graph: StructuralMetadata): Boolean
  def split(graph: StructuralMetadata): Either[String,(StructuralMetadata, StructuralMetadata)]
  def paths: (PartitionTreePath, PartitionTreePath)
  def leftFraction(graph: StructuralMetadata): Double
  def shape(df: DataFrame, graph: StructuralMetadata): TreeNode.AnyNode[DataFrame]
}

final case class PresenceSplitCandidate(
  path: PathKey
) extends SplitCandidate {

  override def isValidFor(graph: StructuralMetadata): Boolean
    = graph.isValidSplitLocation(path)

  override def split(graph: StructuralMetadata): Either[String,(StructuralMetadata, StructuralMetadata)]
    = graph.splitBy(path).map(_.swap)

  override def paths: (PartitionTreePath, PartitionTreePath)
    = (Present, Absent)

  override def leftFraction(graph: StructuralMetadata): Double
    = graph.absoluteProbability(path)
  
  override def shape(df: DataFrame, graph: StructuralMetadata): TreeNode.AnyNode[DataFrame]
    = SplitByPresence(path, df.filter(path.toCol.isNotNull), df.filter(path.toCol.isNull))
}

final case class MedianSplitCandidate(
  path: PathKey,
  quantile: Double = .5
) extends SplitCandidate {

  override def isValidFor(graph: StructuralMetadata): Boolean
    = graph.absoluteProbability(Some(path)) > 0 && graph.separatorForLeaf(Some(path), quantile).isRight
    
  override def split(graph: StructuralMetadata): Either[String,(StructuralMetadata, StructuralMetadata)]
    = graph.splitBy(path, quantile)

  override def paths: (PartitionTreePath, PartitionTreePath) = (Less, MoreOrNull)

  override def leftFraction(graph: StructuralMetadata): Double = {
    val (_, probability) = graph.separatorForLeaf(Some(path), quantile).right.get
    graph.absoluteProbability(path) * probability
  }

  override def shape(df: DataFrame, graph: StructuralMetadata): TreeNode.AnyNode[DataFrame] = {
    val (separator, _) = graph.separatorForLeaf(Some(path), quantile).right.get
    SplitByValue(separator, path, df.filter(path.toCol <= separator.toLiteral), df.filter(path.toCol.isNull || path.toCol > separator.toLiteral))
  }
}

final case class SplitCandidateState(
  graph: StructuralMetadata,
  size: Double,
  path: Seq[PartitionTreePath]
) extends RecursionState {

  override def priority: Double = size

}
