package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import de.unikl.cs.dbis.waves.split.recursive.PresentMetric
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

case class ModelGini(
  maxBuckets: Int
) extends Recursive[SplitCandidateState] with NoPrerequisites {
  import ModelGini._

  require(maxBuckets > 0)

  private var currentBuckets = 1
  private var splitLocations: RDD[SplitCandidate] = null
  private var spark: SparkSession = null

  override protected def initialRecursionState(state: PipelineState): SplitCandidateState = {
    val schema = Schema(state)

    currentBuckets = 1
    spark = state.data.sparkSession
    splitLocations = spark.sparkContext.parallelize[SplitCandidate](
      ObjectCounter.paths(schema).map(PresenceSplitCandidate(_)) ++
      schema.leafPaths.map(MedianSplitCandidate(_))
    ).persist()
    
    val tree = dfToRSIGraph(state.data, schema)
    findBestSplit(tree, Seq.empty).get
  }

  override protected def checkRecursion(recState: SplitCandidateState): Boolean
    = currentBuckets < maxBuckets

  override protected def doRecursionStep(recState: SplitCandidateState, df: DataFrame): Seq[SplitCandidateState] = {
    currentBuckets += 1
    for {
      (childGraph, childPath) <- recState.children
      child <- findBestSplit(childGraph, childPath)
    } yield child
  }

  private def findBestSplit(tree: RSIGraph, path: Seq[PartitionTreePath]): Option[SplitCandidateState] = {
    splitLocations.mapPartitions({ partition =>
      val splits = for {
        candidate <- partition
        split = candidate.split(tree)
        if split.isRight
        (leftSide, rightSide) = split.right.get
      } yield {
        val leftFraction = candidate.leftFraction(tree)
        val gini = leftFraction * leftSide.gini + (1-leftFraction) * rightSide.gini
        (candidate, gini)
      }
      if (splits.isEmpty) Iterator.empty else Iterator(Some(splits.minBy(_._2)): Option[(SplitCandidate, Double)])
    }).fold(None)(mergeOptions({ case (lhs@(_, lhsGini), rhs@(_, rhsGini)) =>
      if (lhsGini < rhsGini) lhs else rhs
    })).map({ case (candidate, gini) =>
      val improvement = tree.gini - gini
      SplitCandidateState(candidate, tree, improvement, path)
    })
  }
}

object ModelGini {

  /**
    * Construct a RSIGraph which holds the metadata of a given DataFrame and
    * schema. This method will perform two scans on the DataFrame
    *
    * @param df the DataFrame to process
    * @param schema the schema of the df
    * @return the constructed RSIGraph
    */
  def dfToRSIGraph(df: DataFrame, schema: StructType): RSIGraph
    = addColumnMetadataToGraph(df,schema,loadRSIGRaph(df, schema))

  private def loadRSIGRaph(df: DataFrame, schema: StructType) = {
    val optionalCount = schema.optionalNodeCount()
    val (rows, counts) = df.rdd.mapPartitions({ partition => 
      val totalCount = ObjectCounter(optionalCount)
      val currentCount = ObjectCounter(optionalCount)
      var rowCount = 0
      for(row <- partition) {
        rowCount += 1
        currentCount <-- PresentMetric(row)
        totalCount += currentCount
      }
      Iterator((rowCount, totalCount))
    }).reduce({ case ((rowsLhs, countsLhs), (rowsRhs, countRhs)) =>
      countsLhs += countRhs
      (rowsLhs + rowsRhs, countsLhs)
    })
    RSIGraph.fromObjectCounter(counts, schema, rows)
  }

  private def addColumnMetadataToGraph(df: DataFrame, schema: StructType, inputGraph: RSIGraph) = {
    val leafs = schema.leafPaths
    val aggs = leafs.map(p => (p.toDotfreeString, "min")) ++
      leafs.map(p => (p.toDotfreeString, "max")) ++
      leafs.map(p => (p.toDotfreeString, "approx_count_distinct"))
    val summary = df.select(leafs.map(p => p.toCol.as(p.toDotfreeString)):_*)
      .agg(aggs.head, aggs.tail:_*)
      .head()
    var graph = inputGraph
    for {
      (leaf, minIndex) <- leafs.zipWithIndex
      maxIndex = minIndex+leafs.length
      distinctIndex = minIndex+2*leafs.length
      meta <- ColumnMetadata.fromRows(summary, minIndex, maxIndex, distinctIndex)
    } {
      graph = graph.setMetadata(Some(leaf), meta).right.get
    }
    graph
  }

  def mergeOptions[A](fn: (A, A) => A)(lhs: Option[A], rhs: Option[A]): Option[A] = {
    if (lhs.isEmpty) return rhs
    if (rhs.isEmpty) return lhs
    Some(fn(lhs.get, rhs.get))
  }
}

sealed trait SplitCandidate {
  def split(graph: RSIGraph): Either[String,(RSIGraph, RSIGraph)]
  def paths: (PartitionTreePath, PartitionTreePath)
  def leftFraction(graph: RSIGraph): Double
  def shape(df: DataFrame, graph: RSIGraph): TreeNode.AnyNode[DataFrame]
}

final case class PresenceSplitCandidate(
  path: PathKey
) extends SplitCandidate {
  override def split(graph: RSIGraph): Either[String,(RSIGraph, RSIGraph)]
    = if (graph.isValidSplitLocation(path)) graph.splitBy(path).map(_.swap) else Left("invalid split location")

  override def paths: (PartitionTreePath, PartitionTreePath)
    = (Present, Absent)

  override def leftFraction(graph: RSIGraph): Double
    = graph.absoluteProbability(path)
  
  override def shape(df: DataFrame, graph: RSIGraph): TreeNode.AnyNode[DataFrame]
    = SplitByPresence(path, df.filter(path.toCol.isNotNull), df.filter(path.toCol.isNull))
}

final case class MedianSplitCandidate(
  path: PathKey,
  quantile: Double = .5
) extends SplitCandidate {
  override def split(graph: RSIGraph): Either[String,(RSIGraph, RSIGraph)]
    = graph.splitBy(path, quantile)

  override def paths: (PartitionTreePath, PartitionTreePath) = (Less, MoreOrNull)

  override def leftFraction(graph: RSIGraph): Double
    = graph.absoluteProbability(path) * quantile

  override def shape(df: DataFrame, graph: RSIGraph): TreeNode.AnyNode[DataFrame] = {
    val separator = graph.separatorForLeaf(Some(path), quantile).right.get
    SplitByValue(separator, path, df.filter(path.toCol <= separator.toLiteral), df.filter(!(path.toCol <= separator.toLiteral)))
  }
}

final case class SplitCandidateState(
  split: SplitCandidate,
  graph: RSIGraph,
  priority: Double,
  path: Seq[PartitionTreePath]
) extends RecursionState {

  override def splitShape(df: DataFrame): TreeNode.AnyNode[DataFrame]
    = split.shape(df, graph)

  def children = {
    val (leftGraph, rightGraph) = split.split(graph).right.get
    val (leftStep, rightStep) = split.paths
    Seq((leftGraph, path :+ leftStep), (rightGraph, path :+ rightStep))
  }
}
