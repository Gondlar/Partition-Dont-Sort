package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions._
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
) extends Recursive[PossibleExactSplit[RSIGraph]] with NoPrerequisites {
  import ModelGini._

  require(maxBuckets > 0)

  private var currentBuckets = 1
  private var splitLocations: RDD[PathKey] = null
  private var spark: SparkSession = null

  override protected def initialRecursionState(state: PipelineState): PossibleExactSplit[RSIGraph] = {
    val schema = Schema(state)

    currentBuckets = 1
    spark = state.data.sparkSession
    splitLocations = spark.sparkContext.parallelize(ObjectCounter.paths(schema)).persist()
    
    val tree = dfToRSIGraph(state.data, schema)
    findBestSplit(tree, Seq.empty).get
  }

  override protected def checkRecursion(recState: PossibleExactSplit[RSIGraph]): Boolean
    = currentBuckets < maxBuckets

  override protected def doRecursionStep(recState: PossibleExactSplit[RSIGraph], df: DataFrame): Seq[PossibleExactSplit[RSIGraph]] = {
    currentBuckets += 1
    recState.info(df).flatMap({ case (_, graph, path) =>
      findBestSplit(graph, path)
    })
  }

  private def findBestSplit(tree: RSIGraph, path: Seq[PartitionTreePath]): Option[PossibleExactSplit[RSIGraph]] = {
    splitLocations.mapPartitions({ partition =>
      val splits = for (path <- partition.filter(!tree.isCertain(_))) yield {
        val probability = tree.absoluteProbability(path)
        val (absent, present) = tree.splitBy(path)
        val gini = probability * present.gini + (1-probability) * absent.gini
        (path, absent, present, gini)
      }
      if (splits.isEmpty) Iterator.empty else Iterator(Some(splits.minBy(_._4)): Option[(PathKey, RSIGraph, RSIGraph, Double)])
    }).fold(None)(mergeOptions({ case (lhs@(_, _, _, lhsGini), rhs@(_, _, _, rhsGini)) =>
      if (lhsGini < rhsGini) lhs else rhs
    })).map({ case (key, absent, present, gini) =>
      val improvement = tree.gini - gini
      PossiblePresenceSplit(improvement, absent, present, path, key)
    })
  }
}

object ModelGini {
  def dfToRSIGraph(df: DataFrame, schema: StructType): RSIGraph = {
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

  def mergeOptions[A](fn: (A, A) => A)(lhs: Option[A], rhs: Option[A]): Option[A] = {
    if (lhs.isEmpty) return rhs
    if (rhs.isEmpty) return lhs
    Some(fn(lhs.get, rhs.get))
  }
}
